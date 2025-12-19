"""
Tests for data persistence - ensuring data survives container restarts.
"""

import pytest
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
async def test_dedup_store_persists_after_connection_reset(db_pool, clean_database):
    """
    Test: Dedup store persists event IDs across connection resets.
    
    Simulates container restart by closing and reopening connection.
    """
    import asyncpg
    from conftest import TEST_DATABASE_URL
    
    # Create a separate pool for this test (don't close fixture pool)
    test_pool = await asyncpg.create_pool(TEST_DATABASE_URL, min_size=2, max_size=10)
    
    try:
        db = Database("")
        db.pool = test_pool
        
        event = create_test_event(event_id="persist_001")
        
        # Process event
        result1 = await db.process_event_idempotent(event)
        assert result1 is True, "First processing should succeed"
        
        # Close pool (simulate restart)
        await test_pool.close()
        
        # Recreate pool (simulate coming back online)
        test_pool = await asyncpg.create_pool(TEST_DATABASE_URL, min_size=2, max_size=10)
        db.pool = test_pool
        
        # Try processing same event again
        result2 = await db.process_event_idempotent(event)
        assert result2 is False, "Should still detect as duplicate after restart"
        
        # Verify data is still there
        async with db.pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = $1",
                "persist_001"
            )
            assert count == 1, "Dedup record should persist"
    
    finally:
        await test_pool.close()


@pytest.mark.asyncio
async def test_event_data_persists(db_pool, clean_database):
    """
    Test: Event data persists and can be queried after storage.
    """
    db = Database("")
    db.pool = db_pool
    
    event = create_test_event(event_id="data_persist_001", topic="persist.test")
    event.payload = {"important": "data", "value": 123}
    
    # Process event
    await db.process_event_idempotent(event)
    
    # Query back
    events = await db.get_events(topic="persist.test")
    
    assert len(events) == 1, "Should find 1 event"
    assert events[0]["event_id"] == "data_persist_001"
    assert events[0]["payload"]["important"] == "data"
    assert events[0]["payload"]["value"] == 123


@pytest.mark.asyncio
async def test_stats_persist_across_queries(db_pool, clean_database):
    """
    Test: Statistics persist and accumulate correctly.
    """
    db = Database("")
    db.pool = db_pool
    
    # Process some events
    for i in range(5):
        event = create_test_event(event_id=f"stats_persist_{i}")
        await db.process_event_idempotent(event)
    
    # Get stats - use timezone-aware datetime
    stats1 = await db.get_stats(datetime.now(timezone.utc))
    assert stats1["unique_processed"] == 5
    
    # Process more events (including a duplicate)
    await db.process_event_idempotent(create_test_event(event_id="stats_persist_0"))  # Duplicate
    await db.process_event_idempotent(create_test_event(event_id="stats_persist_10"))  # New
    
    # Get stats again
    stats2 = await db.get_stats(datetime.now(timezone.utc))
    assert stats2["unique_processed"] == 6, "Should be 6 unique"
    assert stats2["duplicate_dropped"] == 1, "Should have 1 duplicate"
    assert stats2["received_total"] == 7, "Should have received 7 total"


@pytest.mark.asyncio
async def test_query_filters_work_correctly(db_pool, clean_database):
    """
    Test: Event queries with topic filter work correctly.
    """
    db = Database("")
    db.pool = db_pool
    
    # Create events with different topics
    topics = ["topic.a", "topic.b", "topic.a", "topic.c", "topic.a"]
    for i, topic in enumerate(topics):
        event = create_test_event(event_id=f"filter_{i}", topic=topic)
        await db.process_event_idempotent(event)
    
    # Query topic.a (should get 3)
    events_a = await db.get_events(topic="topic.a")
    assert len(events_a) == 3, "Should find 3 events for topic.a"
    assert all(e["topic"] == "topic.a" for e in events_a)
    
    # Query topic.b (should get 1)
    events_b = await db.get_events(topic="topic.b")
    assert len(events_b) == 1, "Should find 1 event for topic.b"
    
    # Query all (no filter)
    events_all = await db.get_events()
    assert len(events_all) == 5, "Should find all 5 events"


@pytest.mark.asyncio
async def test_large_payload_persists(db_pool, clean_database):
    """
    Test: Events with large JSON payloads are stored correctly.
    """
    db = Database("")
    db.pool = db_pool
    
    # Create event with large payload
    large_payload = {
        "users": [{"id": i, "name": f"User {i}", "email": f"user{i}@example.com"} for i in range(100)],
        "metadata": {"key_" + str(i): f"value_{i}" for i in range(50)},
        "description": "x" * 1000  # 1KB string
    }
    
    event = create_test_event(event_id="large_001")
    event.payload = large_payload
    
    # Store
    await db.process_event_idempotent(event)
    
    # Retrieve
    events = await db.get_events()
    assert len(events) == 1
    
    retrieved_payload = events[0]["payload"]
    assert len(retrieved_payload["users"]) == 100
    assert len(retrieved_payload["metadata"]) == 50
    assert len(retrieved_payload["description"]) == 1000
