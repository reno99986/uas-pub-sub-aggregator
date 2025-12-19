"""
Test configuration and fixtures for the test suite.
"""

import pytest
import pytest_asyncio
import asyncio
import asyncpg
import os
from redis import asyncio as aioredis

# Test database configuration
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://agguser:aggpass@localhost:5432/logaggregator"
)
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379")


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def db_pool():
    """Create a database connection pool for tests."""
    pool = await asyncpg.create_pool(TEST_DATABASE_URL, min_size=2, max_size=10)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def redis_client():
    """Create a Redis client for tests."""
    client = await aioredis.from_url(TEST_REDIS_URL, encoding="utf-8", decode_responses=True)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def clean_database(db_pool):
    """Clean the database before each test."""
    async with db_pool.acquire() as conn:
        # Clear all data
        await conn.execute("TRUNCATE events CASCADE")
        await conn.execute("TRUNCATE processed_events CASCADE")
        
        # Reset stats
        await conn.execute("""
            UPDATE stats 
            SET received_count = 0,
                unique_processed_count = 0,
                duplicate_dropped_count = 0
            WHERE id = 1
        """)
    
    yield
    
    # Cleanup after test
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE events CASCADE")
        await conn.execute("TRUNCATE processed_events CASCADE")


@pytest_asyncio.fixture
async def clean_redis(redis_client):
    """Clean Redis queue before each test."""
    await redis_client.delete("events_queue")
    yield
    await redis_client.delete("events_queue")
