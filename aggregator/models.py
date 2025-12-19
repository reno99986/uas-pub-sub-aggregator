"""
Data models for the Log Aggregator system.
Defines Event schema with Pydantic for validation.
"""

from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime, timezone
from typing import Dict, Any, List
import re


class Event(BaseModel):
    """
    Event model representing a single log event.
    
    Schema enforces:
    - topic: Category/type of event
    - event_id: Unique identifier for deduplication
    - timestamp: When the event occurred
    - source: Origin service/system
    - payload: Arbitrary JSON data
    """
    topic: str = Field(..., min_length=1, max_length=255, description="Event topic/category")
    event_id: str = Field(..., min_length=1, max_length=255, description="Unique event identifier")
    timestamp: datetime = Field(..., description="Event timestamp (ISO 8601)")
    source: str = Field(..., min_length=1, max_length=255, description="Event source system")
    payload: Dict[str, Any] = Field(..., description="Event payload data")
    
    @field_validator('topic', 'event_id', 'source')
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Remove leading/trailing whitespace from string fields"""
        return v.strip()
    
    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp_with_timezone(cls, v):
        """Parse timestamp and ensure it's timezone-aware (UTC)"""
        if isinstance(v, str):
            # Parse ISO format string and convert to UTC
            # Handle both 'Z' suffix and '+00:00' format
            dt = datetime.fromisoformat(v.replace('Z', '+00:00'))
            # Explicitly convert to UTC
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        elif isinstance(v, datetime):
            # If already datetime, ensure it's in UTC
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC explicitly
                return v.astimezone(timezone.utc)
        return v

    model_config = ConfigDict(
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "topic": "user.login",
                "event_id": "evt_20231206_abc123",
                "timestamp": "2023-12-06T14:45:22Z",
                "source": "auth-service",
                "payload": {
                    "user_id": 123,
                    "ip": "192.168.1.1",
                    "success": True
                }
            }
        }
    )


class EventBatch(BaseModel):
    """Batch of events for bulk processing"""
    events: List[Event] = Field(..., min_length=1, max_length=1000)


class EventResponse(BaseModel):
    """Response after processing an event"""
    event_id: str
    status: str  # 'processed', 'duplicate', 'error'
    success: bool
    error: str | None = None


class BatchResponse(BaseModel):
    """Response after processing a batch of events"""
    total: int
    success: int
    failed: int
    results: List[EventResponse]


class StatsResponse(BaseModel):
    """System statistics response"""
    received_total: int
    unique_processed: int
    duplicate_dropped: int
    active_topics: int
    uptime_seconds: float
    started_at: datetime
    last_updated: datetime


class EventQueryResponse(BaseModel):
    """Response for event queries"""
    total: int
    events: List[Dict[str, Any]]
    topic_filter: str | None = None
