import asyncio
import uuid
from typing import Optional, Dict
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)


class RedisReentrantLock:
    """
    Redis-based Reentrant (Recursive) Lock implementation using pure Redis commands.

    A reentrant lock allows the same thread/task to acquire the lock multiple times
    without blocking itself. It maintains a counter to track the number of times
    the lock has been acquired.

    Implementation Details:
    - Uses Redis HASH to store owner and acquisition count
    - Uses Redis WATCH/MULTI/EXEC for atomic operations (optimistic locking)
    - All operations use native Redis commands (no Lua scripts)

    Key Features:
    - Same owner can acquire lock multiple times
    - Lock must be released same number of times as acquired
    - Prevents self-deadlock in recursive operations
    - Thread-safe using Redis transactions
    """

    def __init__(
            self,
            redis: aioredis.Redis,
            ttl_ms: int = 30000,
            retry_delay_ms: int = 100,
            max_retries: int = 10
    ):
        """
        Initialize Redis reentrant lock.

        Args:
            redis: Async Redis client
            ttl_ms: Lock time-to-live in milliseconds (default: 30 seconds)
            retry_delay_ms: Base delay between retries in milliseconds
            max_retries: Maximum number of acquisition attempts
        """
        self.redis = redis
        self.ttl_ms = ttl_ms
        self.retry_delay_ms = retry_delay_ms
        self.max_retries = max_retries

    async def acquire(
            self,
            resource: str,
            owner_id: Optional[str] = None,
            max_retries: Optional[int] = None
    ) -> Optional[str]:
        """
        Acquire a reentrant lock on a resource using Redis transactions.

        If the same owner already holds the lock, increment the counter (reentry).
        If the lock is held by another owner, retry until acquired or max retries reached.

        Args:
            resource: Resource identifier to lock
            owner_id: Optional unique owner ID (e.g., thread/task ID)
            max_retries: Override default max_retries

        Returns:
            owner_id on success, None on failure
        """
        owner = owner_id or str(uuid.uuid4())
        lock_key = self._make_lock_key(resource)
        retries = max_retries if max_retries is not None else self.max_retries

        for attempt in range(retries):
            try:
                acquired = await self._try_acquire(lock_key, owner)

                if acquired:
                    count = await self._get_acquisition_count(resource, owner)
                    logger.info(
                        f"Lock acquired on '{resource}' by {owner} "
                        f"(acquisition count: {count})"
                    )
                    return owner

            except aioredis.WatchError:
                logger.debug(f"Transaction conflict on '{resource}', retrying...")
                pass

            delay = (self.retry_delay_ms / 1000.0) * (1.5 ** attempt)
            logger.debug(
                f"Lock '{resource}' held by another owner, "
                f"retry {attempt + 1}/{retries} after {delay:.3f}s"
            )
            await asyncio.sleep(delay)

        logger.warning(f"Failed to acquire lock on '{resource}' after {retries} attempts")
        return None

    async def _try_acquire(self, lock_key: str, owner: str) -> bool:
        """
        Try to acquire lock using Redis WATCH/MULTI/EXEC transaction.

        This provides optimistic locking - if the lock changes between
        WATCH and EXEC, the transaction fails and we retry.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.watch(lock_key)

            try:
                exists = await pipe.exists(lock_key)

                if exists == 0:
                    pipe.multi()
                    await pipe.hset(lock_key, owner, 1)
                    await pipe.pexpire(lock_key, self.ttl_ms)
                    await pipe.execute()
                    return True

                else:
                    current_count = await pipe.hget(lock_key, owner)

                    if current_count:
                        new_count = int(current_count) + 1
                        pipe.multi()
                        await pipe.hset(lock_key, owner, new_count)
                        await pipe.pexpire(lock_key, self.ttl_ms)
                        await pipe.execute()
                        return True

                    else:
                        await pipe.unwatch()
                        return False

            except aioredis.WatchError:
                raise

    async def release(self, resource: str, owner_id: str) -> bool:
        """
        Release a reentrant lock using Redis transactions.

        Decrements the acquisition counter. Lock is fully released only when
        counter reaches 0 (released as many times as acquired).

        Args:
            resource: Resource identifier to unlock
            owner_id: Lock owner ID from acquire

        Returns:
            True if lock still held (counter > 0) or fully released (counter = 0)
            False if lock not owned or already released
        """
        if not owner_id:
            logger.error("Cannot release lock without owner_id")
            return False

        lock_key = self._make_lock_key(resource)

        for attempt in range(3):
            try:
                result = await self._try_release(lock_key, owner_id, resource)
                return result

            except aioredis.WatchError:
                logger.debug(f"Transaction conflict during release, retry {attempt + 1}/3")
                await asyncio.sleep(0.01)
                continue

        logger.warning(f"Failed to release lock on '{resource}' after 3 attempts")
        return False

    async def _try_release(self, lock_key: str, owner_id: str, resource: str) -> bool:
        """
        Try to release lock using Redis WATCH/MULTI/EXEC transaction.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.watch(lock_key)

            try:
                current_count = await pipe.hget(lock_key, owner_id)

                if not current_count:
                    await pipe.unwatch()
                    logger.warning(f"Cannot release lock on '{resource}' - not owner or already released")
                    return False

                new_count = int(current_count) - 1

                pipe.multi()

                if new_count <= 0:
                    await pipe.delete(lock_key)
                    await pipe.execute()
                    logger.info(f"Lock on '{resource}' fully released by {owner_id}")
                    return True
                else:
                    await pipe.hset(lock_key, owner_id, new_count)
                    await pipe.execute()
                    logger.debug(
                        f"Lock on '{resource}' partially released by {owner_id} "
                        f"(remaining count: {new_count})"
                    )
                    return True

            except aioredis.WatchError:
                raise

    async def extend(
            self,
            resource: str,
            owner_id: str,
            additional_ms: Optional[int] = None
    ) -> bool:
        """
        Extend the TTL of a reentrant lock using Redis transactions.

        Args:
            resource: Resource identifier
            owner_id: Lock owner ID
            additional_ms: Additional milliseconds to extend (default: original ttl_ms)

        Returns:
            True if extended successfully
        """
        if not owner_id:
            return False

        lock_key = self._make_lock_key(resource)
        extend_ms = additional_ms if additional_ms is not None else self.ttl_ms

        for attempt in range(3):
            try:
                async with self.redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(lock_key)
                    current_count = await pipe.hget(lock_key, owner_id)

                    if not current_count:
                        await pipe.unwatch()
                        logger.warning(f"Cannot extend lock on '{resource}' - not owner")
                        return False

                    pipe.multi()
                    await pipe.pexpire(lock_key, extend_ms)
                    await pipe.execute()

                    logger.debug(f"Extended lock on '{resource}' by {extend_ms}ms")
                    return True

            except aioredis.WatchError:
                logger.debug(f"Transaction conflict during extend, retry {attempt + 1}/3")
                await asyncio.sleep(0.01)
                continue

        return False

    async def get_lock_info(self, resource: str) -> Optional[Dict]:
        """
        Get information about a lock using Redis commands.

        Args:
            resource: Resource identifier

        Returns:
            Dict with lock info or None if not locked
        """
        lock_key = self._make_lock_key(resource)


        async with self.redis.pipeline(transaction=False) as pipe:
            await pipe.exists(lock_key)
            await pipe.hgetall(lock_key)
            await pipe.pttl(lock_key)
            results = await pipe.execute()

        exists = results[0]
        lock_data = results[1]
        ttl_ms = results[2]

        if not exists or ttl_ms < 0:
            return None

        owners = {}
        if lock_data:
            for owner, count in lock_data.items():
                if isinstance(owner, bytes):
                    owner = owner.decode('utf-8')
                owners[owner] = int(count)

        return {
            "resource": resource,
            "owners": owners,
            "ttl_ms": ttl_ms
        }

    async def _get_acquisition_count(self, resource: str, owner_id: str) -> int:
        """Get the current acquisition count for an owner."""
        lock_key = self._make_lock_key(resource)
        count = await self.redis.hget(lock_key, owner_id)
        return int(count) if count else 0

    async def is_locked(self, resource: str, owner_id: Optional[str] = None) -> bool:
        """
        Check if a resource is locked, optionally by a specific owner.

        Args:
            resource: Resource identifier to check
            owner_id: Optional owner ID to verify ownership

        Returns:
            True if locked (and owned by owner_id if provided)
        """
        lock_key = self._make_lock_key(resource)

        exists = await self.redis.exists(lock_key)
        if not exists:
            return False

        if owner_id:
            count = await self._get_acquisition_count(resource, owner_id)
            return count > 0

        return True

    async def force_release(self, resource: str):
        """
        Force release lock without owner verification (use with caution).

        Args:
            resource: Resource identifier to forcefully unlock
        """
        lock_key = self._make_lock_key(resource)
        await self.redis.delete(lock_key)
        logger.warning(f"Force released lock on '{resource}'")

    def _make_lock_key(self, resource: str) -> str:
        """Create a namespaced lock key."""
        return f"reentrant_lock:{resource}"


@asynccontextmanager
async def reentrant_lock(
        redis: aioredis.Redis,
        resource: str,
        owner_id: Optional[str] = None,
        ttl_ms: int = 30000
):
    """
    Async context manager for reentrant locking with automatic release.

    Usage:
        async with reentrant_lock(redis, "user:123", owner_id="task-1") as owner:
            if owner:
                # Critical section - lock acquired
                await process_user()

                # Can acquire same lock again (reentry)
                async with reentrant_lock(redis, "user:123", owner_id=owner) as owner2:
                    await nested_operation()

    Args:
        redis: Async Redis client
        resource: Resource identifier to lock
        owner_id: Optional owner ID (use same ID for reentry)
        ttl_ms: Lock time-to-live in milliseconds
    """
    lock = RedisReentrantLock(redis, ttl_ms=ttl_ms)
    owner = await lock.acquire(resource, owner_id=owner_id)

    try:
        yield owner
    finally:
        if owner:
            await lock.release(resource, owner)