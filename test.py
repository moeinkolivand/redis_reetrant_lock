import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import redis.asyncio as aioredis

from order.order import Order, OrderStatus, OrderProcessingService
from order.producer import OrderProducer, OrderCreatedEvent, OrderCancelledEvent
from order.consumer import OrderConsumer, OrderCompletedEvent, OrderFailedEvent
from redis_module.redis_reentrant_lock import RedisReentrantLock


@pytest.fixture
async def redis_client():
    client = await aioredis.from_url(
        "redis://localhost:6379/1",
        decode_responses=True,
        encoding="utf8"
    )

    await client.flushdb()

    user_data = {"user_id": "user_1", "full_name": "Test User", "email": "test@example.com"}
    await client.set("user:user_1:json", json.dumps(user_data))

    products = {
        "P1": 50,
        "P2": 100,
        "P3": 75
    }
    for product_id, stock in products.items():
        await client.set(f"product:{product_id}", stock)

    yield client

    await client.flushdb()
    await client.aclose()


@pytest.fixture
def mock_broker():
    broker = MagicMock()
    broker.publish = AsyncMock()
    return broker


@pytest.fixture
async def order_producer(mock_broker, redis_client):
    return OrderProducer(mock_broker, redis_client)


@pytest.fixture
async def order_consumer(mock_broker, redis_client):
    return OrderConsumer(mock_broker, redis_client)


@pytest.fixture
async def order_service(redis_client):
    return OrderProcessingService(redis_client)


class TestOrder:

    def test_order_creation(self):
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
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-001", "user_1", items)

        order_dict = order.to_dict()

        assert order_dict["order_id"] == "ORD-001"
        assert order_dict["user_id"] == "user_1"
        assert "payment_info" in order_dict
        assert "inventory_info" in order_dict
        assert "shipping_info" in order_dict

    def test_add_log(self):
        order = Order("ORD-001", "user_1", [])

        order.add_log("Test message", lock_count=1)

        assert len(order.transaction_log) == 1
        assert order.transaction_log[0]["message"] == "Test message"
        assert order.transaction_log[0]["lock_count"] == 1


class TestOrderProducer:

    @pytest.mark.asyncio
    async def test_create_order(self, order_producer, redis_client):
        items = [{"product_id": "P1", "quantity": 1, "price": 999.99}]

        order = await order_producer.create_order("user_1", items)

        assert order.order_id.startswith("ORD-")
        assert order.user_id == "user_1"
        assert len(order.items) == 1

        saved_order = await redis_client.get(f"order:{order.order_id}")
        assert saved_order is not None

        order_producer.broker.publish.assert_called_once()
        call_args = order_producer.broker.publish.call_args
        assert call_args.kwargs["topic"] == "order.created"

        published_message = call_args.kwargs["message"]
        assert isinstance(published_message, OrderCreatedEvent)
        assert published_message.order_id == order.order_id

    @pytest.mark.asyncio
    async def test_create_order_with_custom_id(self, order_producer):
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]

        order = await order_producer.create_order("user_1", items, order_id="CUSTOM-001")

        assert order.order_id == "CUSTOM-001"

    @pytest.mark.asyncio
    async def test_cancel_order(self, order_producer, redis_client):
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = await order_producer.create_order("user_1", items)

        await order_producer.cancel_order(order.order_id, "Test cancellation")

        saved_order_data = await redis_client.get(f"order:{order.order_id}")
        saved_order = json.loads(saved_order_data)

        assert saved_order["status"] == OrderStatus.FAILED

        assert order_producer.broker.publish.call_count == 2
        cancel_call = order_producer.broker.publish.call_args_list[1]
        assert cancel_call.kwargs["topic"] == "order.cancelled"

        cancel_message = cancel_call.kwargs["message"]
        assert isinstance(cancel_message, OrderCancelledEvent)

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, order_producer):
        with pytest.raises(ValueError, match="Order .* not found"):
            await order_producer.cancel_order("NONEXISTENT", "Test")


class TestOrderConsumer:

    @pytest.mark.asyncio
    async def test_process_order_created_success(self, order_consumer, redis_client):
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

        with patch('order.consumer.context') as mock_context:
            mock_message = MagicMock()
            mock_message.partition = 0
            mock_context.get_local.return_value = mock_message

            await order_consumer.process_order_created(message)

        processed_order_data = await redis_client.get(f"order:{order.order_id}")
        processed_order = json.loads(processed_order_data)

        assert processed_order["status"] == OrderStatus.COMPLETED

        order_consumer.broker.publish.assert_called()
        call_args = order_consumer.broker.publish.call_args
        assert call_args.kwargs["topic"] == "order.completed"

        completed_message = call_args.kwargs["message"]
        assert isinstance(completed_message, OrderCompletedEvent)

    @pytest.mark.asyncio
    async def test_process_order_failure(self, order_consumer, redis_client):
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

        with patch('order.consumer.context') as mock_context:
            mock_message = MagicMock()
            mock_message.partition = 0
            mock_context.get_local.return_value = mock_message

            await order_consumer.process_order_created(message)

        call_args = order_consumer.broker.publish.call_args
        assert call_args.kwargs["topic"] == "order.failed"

        failed_message = call_args.kwargs["message"]
        assert isinstance(failed_message, OrderFailedEvent)


class TestOrderProcessingService:

    @pytest.mark.asyncio
    async def test_process_order_complete_flow(self, order_service, redis_client):
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
        order = Order("ORD-NO-ITEMS", "user_1", [])
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        result = await order_service.validate_order(order.order_id, "test-worker")

        assert result is False

    @pytest.mark.asyncio
    async def test_validate_order_invalid_user(self, order_service, redis_client):
        items = [{"product_id": "P1", "quantity": 1, "price": 99.99}]
        order = Order("ORD-BAD-USER", "nonexistent_user", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        result = await order_service.validate_order(order.order_id, "test-worker")

        assert result is False

    @pytest.mark.asyncio
    async def test_reserve_items_insufficient_stock(self, order_service, redis_client):
        items = [{"product_id": "P1", "quantity": 1000, "price": 99.99}]
        order = Order("ORD-NO-STOCK", "user_1", items)
        await redis_client.set(f"order:{order.order_id}", json.dumps(order.to_dict()))

        result = await order_service.reserve_items(order.order_id, "test-worker")

        assert result is False


class TestReentrantLock:

    @pytest.mark.asyncio
    async def test_lock_acquire_and_release(self, redis_client):
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
        lock = RedisReentrantLock(redis_client, ttl_ms=5000)

        info_empty = await lock.get_lock_info("test_resource")
        assert info_empty is None

        owner = await lock.acquire("test_resource", "owner-1")

        info = await lock.get_lock_info("test_resource")
        assert info is not None
        assert "owner-1" in info["owners"]
        assert info["owners"]["owner-1"] == 1
        assert info["ttl_ms"] > 0

        await lock.release("test_resource", "owner-1")


class TestConcurrentOrderProcessing:

    @pytest.mark.asyncio
    async def test_multiple_workers_same_order(self, order_service, redis_client):
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

    def test_order_created_event(self):
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
        event = OrderFailedEvent(
            order_id="ORD-001",
            reason="Payment failed",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.failed"
        assert event.order_id == "ORD-001"

    def test_order_cancelled_event(self):
        event = OrderCancelledEvent(
            order_id="ORD-001",
            reason="User requested",
            timestamp="2024-01-01T00:00:00"
        )

        assert event.event_type == "order.cancelled"
        assert event.order_id == "ORD-001"