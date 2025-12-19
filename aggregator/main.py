"""
Main entry point for the Log Aggregator service.
Combines FastAPI REST API with background consumer workers.
"""

import asyncio
import structlog
import os
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from redis import asyncio as aioredis
import uvicorn

from models import Event, EventBatch, EventResponse, BatchResponse, StatsResponse, EventQueryResponse
from database import Database
from consumer import start_consumers

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()

# Global state
db: Database | None = None
redis_client: aioredis.Redis | None = None
start_time = datetime.now(timezone.utc)
consumer_task: asyncio.Task | None = None

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://agguser:aggpass@localhost:5432/logaggregator")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "3"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle management"""
    global db, redis_client, consumer_task
    
    # Startup
    logger.info("service_starting", database_url=DATABASE_URL.split('@')[1])
    
    # Connect to database
    db = Database(DATABASE_URL)
    await db.connect()
    
    # Connect to Redis
    redis_client = await aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("redis_connected")
    
    # Start consumer workers in background
    consumer_task = asyncio.create_task(start_consumers(REDIS_URL, db, NUM_WORKERS))
    
    logger.info("service_started", num_workers=NUM_WORKERS)
    
    yield
    
    # Shutdown
    logger.info("service_stopping")
    
    # Cancel consumer task
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    
    # Disconnect
    if redis_client:
        await redis_client.close()
    if db:
        await db.disconnect()
    
    logger.info("service_stopped")


# Create FastAPI app
app = FastAPI(
    title="Distributed Log Aggregator",
    description="Pub-Sub log aggregator with idempotent processing and deduplication",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker healthcheck"""
    db_healthy = await db.health_check() if db else False
    
    if not db_healthy:
        raise HTTPException(status_code=503, detail="Database unhealthy")
    
    return {
        "status": "healthy",
        "database": "connected",
        "redis": "connected",
        "uptime_seconds": (datetime.now(timezone.utc) - start_time).total_seconds()
    }


@app.post("/publish", response_model=EventResponse)
async def publish_event(event: Event):
    """
    Publish a single event.
    
    Event will be added to Redis queue for processing by consumer workers.
    Processing is idempotent - duplicate events (same topic+event_id) are ignored.
    """
    try:
        # Push to Redis queue
        await redis_client.rpush("events_queue", event.model_dump_json())
        
        logger.info(
            "event_published",
            topic=event.topic,
            event_id=event.event_id,
            source=event.source
        )
        
        return EventResponse(
            event_id=event.event_id,
            status="queued",
            success=True
        )
    
    except Exception as e:
        logger.error("publish_error", event_id=event.event_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to publish event: {str(e)}")


@app.post("/publish/batch", response_model=BatchResponse)
async def publish_batch(batch: EventBatch):
    """
    Publish a batch of events.
    
    Each event is processed individually. Partial success is allowed.
    """
    results = []
    
    for event in batch.events:
        try:
            await redis_client.rpush("events_queue", event.model_dump_json())
            
            results.append(EventResponse(
                event_id=event.event_id,
                status="queued",
                success=True
            ))
        
        except Exception as e:
            logger.error("batch_publish_error", event_id=event.event_id, error=str(e))
            results.append(EventResponse(
                event_id=event.event_id,
                status="error",
                success=False,
                error=str(e)
            ))
    
    success_count = sum(1 for r in results if r.success)
    
    logger.info(
        "batch_published",
        total=len(batch.events),
        success=success_count,
        failed=len(batch.events) - success_count
    )
    
    return BatchResponse(
        total=len(batch.events),
        success=success_count,
        failed=len(batch.events) - success_count,
        results=results
    )


@app.get("/events", response_model=EventQueryResponse)
async def get_events(
    topic: str | None = Query(None, description="Filter by topic"),
    limit: int = Query(100, ge=1, le=1000, description="Max results")
):
    """
    Query processed events.
    
    Returns events that have been successfully processed (deduplicated).
    """
    try:
        events = await db.get_events(topic=topic, limit=limit)
        
        return EventQueryResponse(
            total=len(events),
            events=events,
            topic_filter=topic
        )
    
    except Exception as e:
        logger.error("query_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get system statistics.
    
    Returns:
    - received_total: Total events received (including duplicates)
    - unique_processed: Unique events processed
    - duplicate_dropped: Duplicate events ignored
    - active_topics: Number of distinct topics
    - uptime_seconds: Service uptime
    """
    try:
        stats = await db.get_stats(start_time)
        return StatsResponse(**stats)
    
    except Exception as e:
        logger.error("stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Stats failed: {str(e)}")


if __name__ == "__main__":
    # Run with uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
