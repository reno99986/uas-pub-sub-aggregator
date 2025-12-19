"""
Database layer for the Log Aggregator.
Handles connection pooling and idempotent event processing.
"""

import asyncpg
import json
import structlog
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from models import Event

logger = structlog.get_logger()


class Database:
    """Database connection pool manager"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool"""
        logger.info("database_connecting", url=self.database_url.split('@')[1])
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        logger.info("database_connected")
    
    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("database_disconnected")
    
    async def process_event_idempotent(self, event: Event) -> bool:
        """
        Process event with idempotent guarantee using transaction.
        
        Uses INSERT...ON CONFLICT DO NOTHING pattern for deduplication.
        All operations (dedup check, event insert, stats update) are atomic.
        
        Args:
            event: Event to process
            
        Returns:
            True if event was newly processed, False if duplicate detected
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: Try to insert into dedup store
                result = await conn.execute(
                    """
                    INSERT INTO processed_events (topic, event_id)
                    VALUES ($1, $2)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    event.topic,
                    event.event_id
                )
                
                # Step 2: Check if insert succeeded
                # Result format: "INSERT 0 1" means 1 row inserted
                # Result format: "INSERT 0 0" means conflict (duplicate)
                if result == "INSERT 0 0":
                    # Duplicate detected - increment counter
                    await conn.execute(
                        """
                        UPDATE stats 
                        SET duplicate_dropped_count = duplicate_dropped_count + 1,
                            received_count = received_count + 1,
                            last_updated = NOW()
                        WHERE id = 1
                        """
                    )
                    
                    logger.info(
                        "duplicate_detected",
                        topic=event.topic,
                        event_id=event.event_id,
                        source=event.source
                    )
                    return False
                
                # Step 3: New event - insert event data
                # Workaround: convert timestamp to ISO string to avoid timezone issues
                timestamp_str = event.timestamp.isoformat() if hasattr(event.timestamp, 'isoformat') else str(event.timestamp)
                
                await conn.execute(
                    """
                    INSERT INTO events (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    event.topic,
                    event.event_id,
                    timestamp_str,  # Use ISO string instead of datetime object
                    event.source,
                    json.dumps(event.payload)
                )
                
                # Step 4: Update statistics
                await conn.execute(
                    """
                    UPDATE stats 
                    SET received_count = received_count + 1,
                        unique_processed_count = unique_processed_count + 1,
                        last_updated = NOW()
                    WHERE id = 1
                    """
                )
                
                logger.info(
                    "event_processed",
                    topic=event.topic,
                    event_id=event.event_id,
                    source=event.source
                )
                return True
    
    async def get_events(self, topic: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query events from database.
        
        Args:
            topic: Optional topic filter
            limit: Maximum number of results
            
        Returns:
            List of event dictionaries
        """
        query = """
            SELECT topic, event_id, timestamp, source, payload, received_at
            FROM events
        """
        params = []
        
        if topic:
            query += " WHERE topic = $1"
            params.append(topic)
        
        query += " ORDER BY received_at DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            
            return [
                {
                    "topic": row["topic"],
                    "event_id": row["event_id"],
                    # timestamp is stored as TEXT (ISO string), no need for isoformat()
                    "timestamp": row["timestamp"] if isinstance(row["timestamp"], str) else row["timestamp"].isoformat(),
                    "source": row["source"],
                    "payload": json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
                    "received_at": row["received_at"].isoformat()
                }
                for row in rows
            ]
    
    async def get_stats(self, start_time: datetime) -> Dict[str, Any]:
        """
        Get system statistics.
        
        Args:
            start_time: Service start time for uptime calculation
            
        Returns:
            Statistics dictionary
        """
        async with self.pool.acquire() as conn:
            # Get stats from stats table
            stats = await conn.fetchrow(
                """
                SELECT 
                    received_count,
                    unique_processed_count,
                    duplicate_dropped_count,
                    started_at,
                    last_updated
                FROM stats
                WHERE id = 1
                """
            )
            
            # Get count of active topics
            active_topics = await conn.fetchval(
                "SELECT COUNT(DISTINCT topic) FROM events"
            )
            
            uptime = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return {
                "received_total": stats["received_count"],
                "unique_processed": stats["unique_processed_count"],
                "duplicate_dropped": stats["duplicate_dropped_count"],
                "active_topics": active_topics or 0,
                "uptime_seconds": uptime,
                "started_at": stats["started_at"],
                "last_updated": stats["last_updated"]
            }
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error("health_check_failed", error=str(e))
            return False
