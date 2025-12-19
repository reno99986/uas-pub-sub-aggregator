"""
Additional API tests for endpoints and validation.
"""

import pytest
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'aggregator'))

from models import Event, EventBatch


@pytest.mark.asyncio
async def test_event_model_validation():
    """Test: Pydantic Event model validates fields correctly."""
    
    # Valid event
    event = Event(
        topic="test.topic",
        event_id="test_001",
        timestamp=datetime.now(timezone.utc),
        source="test",
        payload={"key": "value"}
    )
    assert event.topic == "test.topic"
    assert event.event_id == "test_001"
    
    # Invalid event - missing required field
    with pytest.raises(Exception):
        Event(
            topic="test.topic",
            # Missing event_id
            timestamp=datetime.now(timezone.utc),
            source="test",
            payload={}
        )


@pytest.mark.asyncio
async def test_event_whitespace_stripping():
    """Test: Whitespace is stripped from string fields."""
    event = Event(
        topic="  test.topic  ",
        event_id="  test_001  ",
        timestamp=datetime.now(timezone.utc),
        source="  test  ",
        payload={}
    )
    
    assert event.topic == "test.topic"
    assert event.event_id == "test_001"
    assert event.source == "test"


@pytest.mark.asyncio
async def test_batch_model_validation():
    """Test: EventBatch model validates correctly."""
    
    events = [
        Event(
            topic=f"topic.{i}",
            event_id=f"evt_{i}",
            timestamp=datetime.now(timezone.utc),
            source="test",
            payload={"index": i}
        )
        for i in range(5)
    ]
    
    batch = EventBatch(events=events)
    assert len(batch.events) == 5
    
    # Empty batch not allowed (min_length=1)
    with pytest.raises(Exception):
        EventBatch(events=[])


@pytest.mark.asyncio
async def test_database_health_check(db_pool):
    """Test: Database health check works correctly."""
    from database import Database
    
    db = Database("")
    db.pool = db_pool
    
    # Should be healthy
    is_healthy = await db.health_check()
    assert is_healthy is True


@pytest.mark.asyncio
async def test_get_events_with_limit(db_pool, clean_database):
    """Test: get_events respects limit parameter."""
    from database import Database
    from models import Event
    from datetime import datetime, timezone
    
    db = Database("")
    db.pool = db_pool
    
    # Create 20 events
    for i in range(20):
        event = Event(
            topic="limit.test",
            event_id=f"limit_{i}",
            timestamp=datetime.now(timezone.utc),
            source="test",
            payload={"index": i}
        )
        await db.process_event_idempotent(event)
    
    # Query with limit=5
    events = await db.get_events(topic="limit.test", limit=5)
    assert len(events) == 5, "Should return only 5 events"
    
    # Query with limit=15
    events = await db.get_events(topic="limit.test", limit=15)
    assert len(events) == 15, "Should return 15 events"
    
    # Query all (default limit=100)
    events = await db.get_events(topic="limit.test")
    assert len(events) == 20, "Should return all 20 events"
