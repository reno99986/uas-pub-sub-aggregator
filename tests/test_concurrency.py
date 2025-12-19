"""
Tests for concurrency control - ensuring multiple workers don't create duplicates.
"""

import pytest
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'aggregator'))

from models import Event
from database import Database


def create_test_event(event_id: str = "test_001", topic: str = "test.topic") -> Event:
    """Helper to create a test event."""
    return Event(
        topic=topic,
        event_id=event_id,
        timestamp=datetime.now(timezone.utc),
        source="test",
        payload={"test": True}
    )


@pytest.mark.asyncio
async def test_concurrent_processing_same_event(db_pool, clean_database):
    """
    Test: Multiple workers processing the same event concurrently.
    
    Expected: Only 1 succeeds, others detect duplicate.
    This tests the race condition prevention.
    """
    db = Database("")
    db.pool = db_pool
    
    event = create_test_event(event_id="concurrent_001")
    
    # Simulate 10 workers processing same event simultaneously
    tasks = [
        db.process_event_idempotent(event)
        for _ in range(10)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Assertions
    assert results.count(True) == 1, "Exactly 1 worker should succeed"
    assert results.count(False) == 9, "9 workers should detect duplicate"
    
    # Verify database has only 1 record
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE event_id = $1",
            "concurrent_001"
        )
        assert count == 1, "Should have exactly 1 record despite 10 attempts"


@pytest.mark.asyncio
async def test_concurrent_different_events(db_pool, clean_database):
    """
    Test: Multiple workers processing different events concurrently.
    
    Expected: All succeed (no false conflicts).
    """
    db = Database("")
    db.pool = db_pool
    
    # Create 20 different events
    events = [create_test_event(event_id=f"evt_{i}") for i in range(20)]
    
    # Process all concurrently
    tasks = [db.process_event_idempotent(event) for event in events]
    results = await asyncio.gather(*tasks)
    
    # All should succeed
    assert all(results), "All unique events should be processed successfully"
    
    # Verify count
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events")
        assert count == 20, "Should have all 20 events"


@pytest.mark.asyncio
async def test_concurrent_mixed_events(db_pool, clean_database):
    """
    Test: Concurrent processing of mix of unique and duplicate events.
    
    Simulates real-world scenario with some duplicates among unique events.
    """
    db = Database("")
    db.pool = db_pool
    
    # Create 10 unique events
    unique_events = [create_test_event(event_id=f"mix_{i}") for i in range(10)]
    
    # Create mix: unique events + duplicates of some
    mixed_events = unique_events.copy()
    mixed_events.extend([unique_events[0], unique_events[1], unique_events[2]])  # 3 duplicates
    
    # Shuffle
    import random
    random.shuffle(mixed_events)
    
    # Process all concurrently
    tasks = [db.process_event_idempotent(event) for event in mixed_events]
    results = await asyncio.gather(*tasks)
    
    # Should have 10 successes and 3 duplicates
    assert results.count(True) == 10, "10 unique events should succeed"
    assert results.count(False) == 3, "3 duplicates should be detected"
    
    # Verify database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events")
        assert count == 10, "Should have 10 unique events"


@pytest.mark.asyncio
async def test_stats_consistency_under_concurrency(db_pool, clean_database):
    """
    Test: Statistics remain consistent under concurrent load.
    
    Critical: Tests that counter updates are atomic (no lost updates).
    """
    db = Database("")
    db.pool = db_pool
    
    # Create 50 events (30 unique + 20 duplicates)
    unique_events = [create_test_event(event_id=f"stats_{i}") for i in range(30)]
    duplicate_events = [unique_events[i % 30] for i in range(20)]
    
    all_events = unique_events + duplicate_events
    import random
    random.shuffle(all_events)
    
    # Process all concurrently (simulating high load)
    tasks = [db.process_event_idempotent(event) for event in all_events]
    await asyncio.gather(*tasks)
    
    # Check stats consistency
    async with db_pool.acquire() as conn:
        stats = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
        
        # Assertions
        assert stats["received_count"] == 50, "Should count all 50 attempts"
        assert stats["unique_processed_count"] == 30, "Should have 30 unique"
        assert stats["duplicate_dropped_count"] == 20, "Should have 20 duplicates"
        
        # Verify: received = unique + duplicates
        assert (stats["unique_processed_count"] + stats["duplicate_dropped_count"] ==
                stats["received_count"]), "Stats should be internally consistent"


@pytest.mark.asyncio
async def test_no_deadlocks_with_many_concurrent_transactions(db_pool, clean_database):
    """
    Test: System handles many concurrent transactions without deadlocks.
    
    Tests that our transaction isolation level and pattern is safe.
    """
    db = Database("")
    db.pool = db_pool
    
    # Create 100 events (some duplicates to create contention)
    events = []
    for i in range(100):
        if i < 70:
            # Unique events
            events.append(create_test_event(event_id=f"deadlock_{i}"))
        else:
            # Duplicate some earlier events to create contention
            events.append(create_test_event(event_id=f"deadlock_{i % 20}"))
    
    import random
    random.shuffle(events)
    
    # Process with high concurrency
    tasks = [db.process_event_idempotent(event) for event in events]
    
    try:
        # This should complete without hanging or deadlock errors
        results = await asyncio.wait_for(
            asyncio.gather(*tasks),
            timeout=30.0  # 30 seconds should be plenty
        )
        
        # Verify we got results
        assert len(results) == 100, "Should get result for all 100 events"
        
        # Verify stats are consistent
        async with db_pool.acquire() as conn:
            stats = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
            assert stats["received_count"] == 100
    
    except asyncio.TimeoutError:
        pytest.fail("Deadlock detected: operations did not complete in time")
