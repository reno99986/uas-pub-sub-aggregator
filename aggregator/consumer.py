"""
Redis consumer workers for processing events from the broker.
Implements multi-worker pattern with idempotent processing.
"""

import asyncio
import json
import structlog
from redis import asyncio as aioredis
from models import Event
from database import Database

logger = structlog.get_logger()


class Consumer:
    """Redis consumer for processing events from broker"""
    
    def __init__(self, redis_url: str, database: Database, worker_id: int):
        self.redis_url = redis_url
        self.database = database
        self.worker_id = worker_id
        self.redis: aioredis.Redis | None = None
        self.running = False
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        logger.info("consumer_connected", worker_id=self.worker_id)
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis:
            await self.redis.close()
            logger.info("consumer_disconnected", worker_id=self.worker_id)
    
    async def start(self):
        """Start consuming events from Redis queue"""
        self.running = True
        logger.info("consumer_started", worker_id=self.worker_id)
        
        while self.running:
            try:
                # Blocking pop from Redis list (timeout 1 second)
                result = await self.redis.blpop("events_queue", timeout=1)
                
                if result is None:
                    # No message, continue
                    continue
                
                # result is tuple: (queue_name, message)
                _, message = result
                
                # Parse event
                event_data = json.loads(message)
                event = Event(**event_data)
                
                # Process idempotently
                is_new = await self.database.process_event_idempotent(event)
                
                if is_new:
                    logger.info(
                        "consumer_processed_new",
                        worker_id=self.worker_id,
                        event_id=event.event_id,
                        topic=event.topic
                    )
                else:
                    logger.info(
                        "consumer_skipped_duplicate",
                        worker_id=self.worker_id,
                        event_id=event.event_id,
                        topic=event.topic
                    )
            
            except json.JSONDecodeError as e:
                logger.error(
                    "invalid_json",
                    worker_id=self.worker_id,
                    error=str(e)
                )
            except Exception as e:
                logger.error(
                    "consumer_error",
                    worker_id=self.worker_id,
                    error=str(e),
                    error_type=type(e).__name__
                )
                # Backoff on error
                await asyncio.sleep(1)
    
    async def stop(self):
        """Stop consuming"""
        self.running = False
        logger.info("consumer_stopped", worker_id=self.worker_id)


async def start_consumers(redis_url: str, database: Database, num_workers: int = 3):
    """
    Start multiple consumer workers.
    
    Args:
        redis_url: Redis connection URL
        database: Database instance
        num_workers: Number of parallel workers
    """
    consumers = []
    
    # Create and connect consumers
    for i in range(num_workers):
        consumer = Consumer(redis_url, database, i)
        await consumer.connect()
        consumers.append(consumer)
    
    # Start all consumers concurrently
    logger.info("starting_consumers", count=num_workers)
    
    try:
        await asyncio.gather(*[consumer.start() for consumer in consumers])
    finally:
        # Cleanup on shutdown
        for consumer in consumers:
            await consumer.stop()
            await consumer.disconnect()
