import pytest_asyncio
import pytest
import asyncio
import json
import redis.asyncio as aioredis
from faststream.kafka import TestKafkaBroker, KafkaBroker

from order.order import Order, OrderStatus, OrderProcessingService
from producer import OrderProducer, OrderCreatedEvent, OrderCancelledEvent
from consumer import OrderConsumer, OrderCompletedEvent, OrderFailedEvent
from redis_module.redis_reentrant_lock import RedisReentrantLock


@pytest_asyncio.fixture
async def redis_client():
    """Setup Redis client and test data"""
    client = await aioredis.from_url(
        "redis://localhost:6379/1",
        decode_responses=True,
        encoding="utf8"
    )

    await client.flushdb()

    user_data = {"user_id": "user_1", "full_name": "Test User", "email": "test@example.com"}
    await client.set("user:user_1:json", json.dumps(user_data))

    products = {
        "P1": {"name": "Laptop", "stock": 50, "price": 999.99},
        "P2": {"name": "Mouse", "stock": 100, "price": 29.99},
        "P3": {"name": "Keyboard", "stock": 75, "price": 79.99}
    }

    for product_id, product_data in products.items():
        await client.set(f"product:{product_id}", product_data["stock"])
        await client.set(f"product:{product_id}:info", json.dumps(product_data))

    yield client

    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
def kafka_broker():
    """Create a Kafka broker for testing"""
    return KafkaBroker()


@pytest_asyncio.fixture
async def order_producer(kafka_broker, redis_client):
    """Create order producer with test broker"""
    return OrderProducer(kafka_broker, redis_client)


@pytest_asyncio.fixture
async def order_consumer(kafka_broker, redis_client):
    """Create order consumer with test broker"""
    return OrderConsumer(kafka_broker, redis_client)


@pytest_asyncio.fixture
async def order_service(redis_client):
    """Create order processing service"""
    return OrderProcessingService(redis_client)


class TestOrder:
    """Test Order entity"""

    def test_order_creation(self):
        """Test order creation with items"""
        items = [
            {"product_id": "P1", "quantity": 1, "price": 999.99},
            {"product_id": "P2", "quantity": 2, "price": 29.99}
        ]

        order = Order("ORD-001", "user_1", items)

        assert order.order_id == "ORD-001"
        assert order.user_id == "user_1"
        assert len(order.items) == 2
        assert order.status == OrderStatus.PENDING
        assert order.payment_info["amount"] == 1059.97

    def test_order_to_dict(self):
        """Test order serialization to dictionary"""
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-001", "user_1", items)

        order_dict = order.to_dict()

        assert order_dict["order_id"] == "ORD-001"
        assert order_dict["user_id"] == "user_1"
        assert "payment_info" in order_dict
        assert "inventory_info" in order_dict
        assert "shipping_info" in order_dict

    def test_add_log(self):
        """Test transaction log addition"""
        order = Order("ORD-001", "user_1", [])

        order.add_log("Test message", lock_count=1)

        assert len(order.transaction_log) == 1
        assert order.transaction_log[0]["message"] == "Test message"
        assert order.transaction_log[0]["lock_count"] == 1


class TestOrderProducer:
    """Test Order Producer with FastStream"""

    @pytest.mark.asyncio
    async def test_create_order(self, order_producer, redis_client, kafka_broker):
        """Test order creation and event publishing"""
        items = [{"product_id": "P1", "quantity": 1, "price": 999.99}]

        async with TestKafkaBroker(kafka_broker) as br:
            order = await order_producer.create_order("user_1", items)

            assert order.order_id.startswith("ORD-")
            assert order.user_id == "user_1"
            assert len(order.items) == 1

            saved_order = await redis_client.get(f"order:{order.order_id}")
            assert saved_order is not None

            await br.publish(
                OrderCreatedEvent(
                    order_id=order.order_id,
                    user_id="user_1",
                    items=items,
                    total_amount=999.99,
                    status="pending",
                    timestamp=order.created_at
                ),
                topic="order.created"
            )

    @pytest.mark.asyncio
    async def test_create_order_with_custom_id(self, order_producer, kafka_broker):
        """Test order creation with custom order ID"""
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        async with TestKafkaBroker(kafka_broker) as br:
            order = await order_producer.create_order("user_1", items, order_id="CUSTOM-001")

        assert order.order_id == "CUSTOM-001"

    @pytest.mark.asyncio
    async def test_cancel_order(self, order_producer, redis_client, kafka_broker):
        """Test order cancellation"""
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]

        async with TestKafkaBroker(kafka_broker):
            order = await order_producer.create_order("user_1", items)
            await order_producer.cancel_order(order.order_id, "Test cancellation")

            saved_order_data = await redis_client.get(f"order:{order.order_id}")
            saved_order = json.loads(saved_order_data)

            assert saved_order["status"] == OrderStatus.FAILED

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, order_producer):
        """Test cancelling non-existent order raises error"""
        with pytest.raises(ValueError, match="Order .* not found"):
            await order_producer.cancel_order("NONEXISTENT", "Test")


class TestOrderConsumer:
    """Test Order Consumer with FastStream"""

    @pytest.mark.asyncio
    async def test_process_order_created_success(self, order_consumer, redis_client, kafka_broker):
        """Test successful order processing"""
        items = [{"product_id": "P1", "quantity": 1, "price": 999.99}]
        order = Order("ORD-TEST-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        message = OrderCreatedEvent(
            event_type="order.created",
            order_id=order.order_id,
            user_id="user_1",
            items=items,
            total_amount=999.99,
            status="pending",
            timestamp="2024-01-01T00:00:00"
        )

        async with TestKafkaBroker(kafka_broker):
            await order_consumer.process_order_created(message, kafka_message=None)

            processed_order_data = await redis_client.get(f"order:{order.order_id}")
            processed_order = json.loads(processed_order_data)

            assert processed_order["status"] == OrderStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_process_order_failure(self, order_consumer, redis_client, kafka_broker):
        """Test order processing failure with invalid user"""
        items = [{"product_id": "P1", "quantity": 1, "price": 999.99}]
        order = Order("ORD-TEST-002", "invalid_user", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        message = OrderCreatedEvent(
            event_type="order.created",
            order_id=order.order_id,
            user_id="invalid_user",
            items=items,
            total_amount=999.99,
            status="pending",
            timestamp="2024-01-01T00:00:00"
        )

        async with TestKafkaBroker(kafka_broker):
            await order_consumer.process_order_created(message, kafka_message=None)

            processed_order_data = await redis_client.get(f"order:{order.order_id}")
            processed_order = json.loads(processed_order_data)

            assert processed_order is not None


class TestOrderProcessingService:
    """Test Order Processing Service"""

    @pytest.mark.asyncio
    async def test_process_order_complete_flow(self, order_service, redis_client):
        """Test complete order processing flow"""
        items = [
            {"product_id": "P1", "quantity": 1, "price": 999.99},
            {"product_id": "P2", "quantity": 2, "price": 29.99}
        ]
        order = Order("ORD-FLOW-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        success = await order_service.process_order(order.order_id, "test-worker-001")

        assert success is True

        final_order = await order_service.get_order(order.order_id)
        assert final_order.status == OrderStatus.COMPLETED
        assert final_order.payment_info["status"] == "completed"
        assert final_order.inventory_info["status"] == "allocated"
        assert final_order.shipping_info["status"] == "scheduled"
        assert final_order.shipping_info["tracking_number"] is not None

    @pytest.mark.asyncio
    async def test_validate_order_no_items(self, order_service, redis_client):
        """Test order validation fails with no items"""
        order = Order("ORD-NO-ITEMS", "user_1", [])
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        result = await order_service.validate_order(order.order_id, "test-worker")

        assert result is False

    @pytest.mark.asyncio
    async def test_validate_order_invalid_user(self, order_service, redis_client):
        """Test order validation fails with invalid user"""
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-BAD-USER", "nonexistent_user", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        result = await order_service.validate_order(order.order_id, "test-worker")

        assert result is False

    @pytest.mark.asyncio
    async def test_reserve_items_insufficient_stock(self, order_service, redis_client):
        """Test item reservation fails with insufficient stock"""
        items = [{"product_id": "P1", "quantity": 1000, "price": 99.99}]
        order = Order("ORD-NO-STOCK", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        await order_service.lock.acquire(f"order:{order.order_id}", "test-worker")
        result = await order_service.reserve_items(order.order_id, "test-worker")
        await order_service.lock.release(f"order:{order.order_id}", "test-worker")

        assert result is False


class TestReentrantLock:
    """Test Reentrant Lock Implementation"""

    @pytest.mark.asyncio
    async def test_lock_acquire_and_release(self, redis_client):
        """Test basic lock acquisition and release"""
        lock = RedisReentrantLock(redis_client, ttl_ms=5000)

        owner = await lock.acquire("test_resource", "owner-1")
        assert owner == "owner-1"

        is_locked = await lock.is_locked("test_resource", "owner-1")
        assert is_locked is True

        released = await lock.release("test_resource", "owner-1")
        assert released is True

        is_locked_after = await lock.is_locked("test_resource", "owner-1")
        assert is_locked_after is False

    @pytest.mark.asyncio
    async def test_lock_reentry(self, redis_client):
        """Test lock reentry (same owner acquires multiple times)"""
        lock = RedisReentrantLock(redis_client, ttl_ms=5000)

        owner1 = await lock.acquire("test_resource", "owner-1")
        assert owner1 == "owner-1"

        owner2 = await lock.acquire("test_resource", "owner-1")
        assert owner2 == "owner-1"

        count = await lock._get_acquisition_count("test_resource", "owner-1")
        assert count == 2

        await lock.release("test_resource", "owner-1")
        count_after_one_release = await lock._get_acquisition_count("test_resource", "owner-1")
        assert count_after_one_release == 1

        await lock.release("test_resource", "owner-1")
        count_after_full_release = await lock._get_acquisition_count("test_resource", "owner-1")
        assert count_after_full_release == 0

    @pytest.mark.asyncio
    async def test_lock_blocked_by_different_owner(self, redis_client):
        """Test lock blocks different owner"""
        lock = RedisReentrantLock(redis_client, ttl_ms=5000, max_retries=2)

        owner1 = await lock.acquire("test_resource", "owner-1")
        assert owner1 == "owner-1"

        owner2 = await lock.acquire("test_resource", "owner-2", max_retries=1)
        assert owner2 is None

        await lock.release("test_resource", "owner-1")

        owner2_retry = await lock.acquire("test_resource", "owner-2")
        assert owner2_retry == "owner-2"

        await lock.release("test_resource", "owner-2")

    @pytest.mark.asyncio
    async def test_lock_extend(self, redis_client):
        """Test lock TTL extension"""
        lock = RedisReentrantLock(redis_client, ttl_ms=2000)

        owner = await lock.acquire("test_resource", "owner-1")
        assert owner == "owner-1"

        info_before = await lock.get_lock_info("test_resource")
        ttl_before = info_before["ttl_ms"]

        extended = await lock.extend("test_resource", "owner-1", 10000)
        assert extended is True

        info_after = await lock.get_lock_info("test_resource")
        ttl_after = info_after["ttl_ms"]

        assert ttl_after > ttl_before

        await lock.release("test_resource", "owner-1")

    @pytest.mark.asyncio
    async def test_lock_info(self, redis_client):
        """Test getting lock information"""
        lock = RedisReentrantLock(redis_client, ttl_ms=5000)

        info_empty = await lock.get_lock_info("test_resource")
        assert info_empty is None

        await lock.acquire("test_resource", "owner-1")

        info = await lock.get_lock_info("test_resource")
        assert info is not None
        assert "owner-1" in info["owners"]
        assert info["owners"]["owner-1"] == 1
        assert info["ttl_ms"] > 0

        await lock.release("test_resource", "owner-1")

    @pytest.mark.asyncio
    async def test_concurrent_workers_only_one_claims(self, redis_client, order_service):
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-CONCURRENT-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        async def worker(name):
            return name, await order_service.process_order(order.order_id, name)

        results = await asyncio.gather(
            worker("worker-1"),
            worker("worker-2"),
            worker("worker-3"),
            worker("worker-4"),
        )
        print(results)
        successful = [w for w, ok in results if ok]

        assert len(successful) == 1

class TestConcurrentOrderProcessing:
    """Test Concurrent Order Processing"""

    @pytest.mark.asyncio
    async def test_multiple_workers_same_order(self, order_service, redis_client):
        """Test multiple workers trying to process same order (only one should succeed)"""
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-CONCURRENT-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        async def process_with_delay(worker_id: str):
            success = await order_service.process_order(order.order_id, worker_id)
            return (worker_id, success)

        results = await asyncio.gather(
            process_with_delay("worker-1"),
            process_with_delay("worker-2"),
            process_with_delay("worker-3")
        )
        successful_workers = [worker for worker, success in results if success]

        assert len(successful_workers) == 1

    @pytest.mark.asyncio
    async def test_multiple_orders_parallel(self, order_service, redis_client):
        """Test multiple orders processed in parallel"""
        orders = []
        for i in range(3):
            items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
            order = Order(f"ORD-PARALLEL-{i}", "user_1", items)
            await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))
            orders.append(order)

        tasks = [
            order_service.process_order(order.order_id, f"worker-{i}")
            for i, order in enumerate(orders)
        ]

        results = await asyncio.gather(*tasks)

        assert all(results)

        for order in orders:
            final_order = await order_service.get_order(order.order_id)
            assert final_order.status == OrderStatus.COMPLETED


class TestPydanticEvents:
    """Test Pydantic Event Models"""

    def test_order_created_event(self):
        """Test OrderCreatedEvent model"""
        event = OrderCreatedEvent(
            order_id="ORD-001",
            user_id="user_1",
            items=[{"product_id": "P1", "quantity": 1, "price": 99.99}],
            total_amount=99.99,
            status="pending",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.created"
        assert event.order_id == "ORD-001"

    def test_order_completed_event(self):
        """Test OrderCompletedEvent model"""
        event = OrderCompletedEvent(
            order_id="ORD-001",
            status="completed",
            payment_transaction_id="TXN-123",
            tracking_number="TRACK-456",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.completed"
        assert event.order_id == "ORD-001"

    def test_order_failed_event(self):
        """Test OrderFailedEvent model"""
        event = OrderFailedEvent(
            order_id="ORD-001",
            reason="Payment failed",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.failed"
        assert event.order_id == "ORD-001"

    def test_order_cancelled_event(self):
        """Test OrderCancelledEvent model"""
        event = OrderCancelledEvent(
            order_id="ORD-001",
            reason="User requested",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.cancelled"
        assert event.order_id == "ORD-001"


class TestStockConsistency:
    """Test Stock Counter and Info JSON Consistency"""

    @pytest.mark.asyncio
    async def test_stock_consistency_after_reservation(self, order_service, redis_client):
        """Test that stock counter and info JSON stay in sync"""
        items = [{"product_id": "P1", "quantity": 5, "price": 999.99}]
        order = Order("ORD-CONSISTENCY-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        initial_stock = int(await redis_client.get("product:P1"))

        await order_service.lock.acquire(f"order:{order.order_id}", "test-worker")
        success = await order_service.reserve_items(order.order_id, "test-worker")
        await order_service.lock.release(f"order:{order.order_id}", "test-worker")

        assert success is True

        final_stock = int(await redis_client.get("product:P1"))
        final_info = json.loads(await redis_client.get("product:P1:info"))

        assert final_stock == initial_stock - 5
        assert final_info["stock"] == initial_stock - 5
        assert final_stock == final_info["stock"]

    @pytest.mark.asyncio
    async def test_stock_consistency_after_rollback(self, order_service, redis_client):
        """Test that stock counter and info JSON stay in sync after rollback"""
        items = [
            {"product_id": "P1", "quantity": 5, "price": 999.99},
            {"product_id": "P2", "quantity": 200, "price": 29.99}  # Will fail
        ]
        order = Order("ORD-ROLLBACK-001", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        initial_p1_stock = int(await redis_client.get("product:P1"))
        initial_p1_info = json.loads(await redis_client.get("product:P1:info"))

        await order_service.lock.acquire(f"order:{order.order_id}", "test-worker")
        success = await order_service.reserve_items(order.order_id, "test-worker")
        await order_service.lock.release(f"order:{order.order_id}", "test-worker")

        assert success is False

        final_p1_stock = int(await redis_client.get("product:P1"))
        final_p1_info = json.loads(await redis_client.get("product:P1:info"))

        assert final_p1_stock == initial_p1_stock
        assert final_p1_info["stock"] == initial_p1_info["stock"]
        assert final_p1_stock == final_p1_info["stock"]  # Must match!
