"""
Publisher service for generating and sending events.
Intentionally creates duplicates to test idempotency.
"""

import asyncio
import random
import string
import json
import structlog
import os
from datetime import datetime, timezone
from redis import asyncio as aioredis

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()

# Configuration from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.35"))  # 35% duplicates
SEND_RATE = int(os.getenv("SEND_RATE", "100"))  # events per second

# Topic categories for simulation
TOPICS = [
    "user.login",
    "user.logout",
    "user.register",
    "order.created",
    "order.completed",
    "order.cancelled",
    "payment.success",
    "payment.failed",
    "inventory.updated",
    "notification.sent"
]


def generate_event_id() -> str:
    """Generate a unique event ID"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"evt_{timestamp}_{random_suffix}"


def generate_event(event_id: str | None = None) -> dict:
    """
    Generate a random event.
    
    Args:
        event_id: If provided, use this event_id (for creating duplicates)
    
    Returns:
        Event dictionary
    """
    topic = random.choice(TOPICS)
    
    if event_id is None:
        event_id = generate_event_id()
    
    # Generate topic-specific payload
    if topic.startswith("user."):
        payload = {
            "user_id": random.randint(1000, 9999),
            "ip": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "user_agent": random.choice(["Chrome/91.0", "Firefox/89.0", "Safari/14.1"])
        }
    elif topic.startswith("order."):
        payload = {
            "order_id": f"ORD-{random.randint(10000,99999)}",
            "user_id": random.randint(1000, 9999),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "items": random.randint(1, 10)
        }
    elif topic.startswith("payment."):
        payload = {
            "transaction_id": f"TXN-{random.randint(10000,99999)}",
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"])
        }
    elif topic.startswith("inventory."):
        payload = {
            "product_id": f"PROD-{random.randint(100,999)}",
            "quantity": random.randint(-10, 100),
            "warehouse": random.choice(["WH-A", "WH-B", "WH-C"])
        }
    else:
        payload = {
            "message": random.choice(["Order shipped", "Account verified", "Password changed"]),
            "priority": random.choice(["low", "medium", "high"])
        }
    
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-service",
        "payload": payload
    }


async def send_events():
    """Main function to generate and send events"""
    logger.info(
        "publisher_starting",
        total_events=TOTAL_EVENTS,
        duplicate_rate=DUPLICATE_RATE,
        send_rate=SEND_RATE
    )
    
    # Connect to Redis
    redis = await aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("redis_connected")
    
    try:
        # Calculate unique vs duplicate counts
        unique_count = int(TOTAL_EVENTS * (1 - DUPLICATE_RATE))
        duplicate_count = TOTAL_EVENTS - unique_count
        
        logger.info(
            "event_plan",
            unique=unique_count,
            duplicates=duplicate_count
        )
        
        # Generate unique events first
        unique_events = []
        for i in range(unique_count):
            event = generate_event()
            unique_events.append(event)
        
        logger.info("unique_events_generated", count=len(unique_events))
        
        # Create duplicates by randomly selecting from unique events
        duplicate_events = []
        for _ in range(duplicate_count):
            # Pick a random unique event and copy it
            original = random.choice(unique_events)
            # Create duplicate with same event_id
            duplicate = generate_event(event_id=original["event_id"])
            # Keep same topic to ensure (topic, event_id) pair matches
            duplicate["topic"] = original["topic"]
            duplicate_events.append(duplicate)
        
        logger.info("duplicate_events_generated", count=len(duplicate_events))
        
        # Combine and shuffle
        all_events = unique_events + duplicate_events
        random.shuffle(all_events)
        
        logger.info("events_shuffled", total=len(all_events))
        
        # Send events at specified rate
        sent_count = 0
        start_time = asyncio.get_event_loop().time()
        
        for event in all_events:
            # Push to Redis queue
            await redis.rpush("events_queue", json.dumps(event))
            
            sent_count += 1
            
            # Log progress every 1000 events
            if sent_count % 1000 == 0:
                elapsed = asyncio.get_event_loop().time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                logger.info(
                    "progress",
                    sent=sent_count,
                    total=TOTAL_EVENTS,
                    rate_per_sec=round(rate, 2)
                )
            
            # Rate limiting
            if SEND_RATE > 0:
                await asyncio.sleep(1.0 / SEND_RATE)
        
        # Final stats
        elapsed = asyncio.get_event_loop().time() - start_time
        actual_rate = sent_count / elapsed if elapsed > 0 else 0
        
        logger.info(
            "publishing_complete",
            total_sent=sent_count,
            duration_seconds=round(elapsed, 2),
            avg_rate_per_sec=round(actual_rate, 2),
            unique=unique_count,
            duplicates=duplicate_count
        )
    
    except Exception as e:
        logger.error("publishing_error", error=str(e), error_type=type(e).__name__)
    
    finally:
        await redis.close()
        logger.info("redis_disconnected")


async def main():
    """Entry point"""
    # Wait a bit for aggregator to be ready
    logger.info("waiting_for_aggregator")
    await asyncio.sleep(10)
    
    # Send events
    await send_events()
    
    logger.info("publisher_finished")


if __name__ == "__main__":
    asyncio.run(main())
