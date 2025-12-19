"""
Tests for idempotency - ensuring duplicate events are not processed twice.
"""

import pytest
import sys
import os
from datetime import datetime, timezone

# Add parent directory to path to import modules
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
        payload={"test": True, "data": "test_data"}
    )


@pytest.mark.asyncio
async def test_duplicate_event_not_processed_twice(db_pool, clean_database):
    """
    Test: Duplicate events (same topic + event_id) are only processed once.
    
    This is the core idempotency test.
    """
    db = Database("")
    db.pool = db_pool
    
    event = create_test_event(event_id="dup_001")
    
    # Process first time
    result1 = await db.process_event_idempotent(event)
    assert result1 is True, "First processing should succeed"
    
    # Process second time (duplicate)
    result2 = await db.process_event_idempotent(event)
    assert result2 is False, "Duplicate should be detected"
    
    # Verify only 1 record in database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE event_id = $1",
            "dup_001"
        )
        assert count == 1, "Should have exactly 1 record"


@pytest.mark.asyncio
async def test_different_topics_same_event_id_allowed(db_pool, clean_database):
    """
    Test: Same event_id but different topics are treated as different events.
    
    Deduplication is on (topic, event_id) pair, not just event_id.
    """
    db = Database("")
    db.pool = db_pool
    
    event1 = create_test_event(event_id="shared_001", topic="topic.a")
    event2 = create_test_event(event_id="shared_001", topic="topic.b")
    
    result1 = await db.process_event_idempotent(event1)
    result2 = await db.process_event_idempotent(event2)
    
    assert result1 is True, "First event should succeed"
    assert result2 is True, "Second event (different topic) should also succeed"
    
    # Verify 2 records
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE event_id = $1",
            "shared_001"
        )
        assert count == 2, "Should have 2 records (different topics)"


@pytest.mark.asyncio
async def test_stats_updated_correctly_for_duplicates(db_pool, clean_database):
    """
    Test: Statistics are correctly updated when duplicates are detected.
    """
    db = Database("")
    db.pool = db_pool
    
    event = create_test_event(event_id="stats_001")
    
    # Process 3 times (1 new + 2 duplicates)
    await db.process_event_idempotent(event)
    await db.process_event_idempotent(event)
    await db.process_event_idempotent(event)
    
    # Check stats
    async with db_pool.acquire() as conn:
        stats = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
        
        assert stats["received_count"] == 3, "Should count all 3 attempts"
        assert stats["unique_processed_count"] == 1, "Only 1 unique event"
        assert stats["duplicate_dropped_count"] == 2, "2 duplicates dropped"


@pytest.mark.asyncio
async def test_multiple_different_events_all_processed(db_pool, clean_database):
    """
    Test: Multiple different events are all processed successfully.
    """
    db = Database("")
    db.pool = db_pool
    
    events = [
        create_test_event(event_id=f"evt_{i}", topic=f"topic.{i}")
        for i in range(10)
    ]
    
    results = []
    for event in events:
        result = await db.process_event_idempotent(event)
        results.append(result)
    
    # All should be True (all unique)
    assert all(results), "All unique events should be processed"
    
    # Verify count
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events")
        assert count == 10, "Should have 10 events"


@pytest.mark.asyncio
async def test_idempotency_with_identical_payload(db_pool, clean_database):
    """
    Test: Events with same topic+event_id but different payload are still deduplicated.
    
    Deduplication is based on (topic, event_id), not payload content.
    """
    db = Database("")
    db.pool = db_pool
    
    event1 = create_test_event(event_id="payload_001")
    event1.payload = {"version": 1}
    
    event2 = create_test_event(event_id="payload_001")
    event2.payload = {"version": 2, "different": "data"}
    
    result1 = await db.process_event_idempotent(event1)
    result2 = await db.process_event_idempotent(event2)
    
    assert result1 is True, "First event should be processed"
    assert result2 is False, "Second event should be rejected (duplicate event_id)"
    
    # Verify stored payload is from first event
    async with db_pool.acquire() as conn:
        payload = await conn.fetchval(
            "SELECT payload FROM events WHERE event_id = $1",
            "payload_001"
        )
        import json
        stored = json.loads(payload) if isinstance(payload, str) else payload
        assert stored["version"] == 1, "Should keep first event's payload"
