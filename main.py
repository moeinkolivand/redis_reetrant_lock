import logging
from typing import Optional
import redis.asyncio as aioredis
from faststream.kafka import KafkaBroker
from faststream import FastStream

from producer import OrderProducer, OrderCreatedEvent, OrderCancelledEvent
from consumer import OrderConsumer, OrderCompletedEvent, OrderFailedEvent
from redis_module.redis_seeder import seed_test_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS: list[str] = ["localhost:9092", "localhost:9094", "localhost:9096"]

kafka_broker = KafkaBroker(KAFKA_BROKERS)
app = FastStream(kafka_broker)

redis_client: Optional[aioredis.Redis] = None
order_consumer: Optional[OrderConsumer] = None


async def get_redis() -> aioredis.Redis:
    global redis_client
    if redis_client is None:
        redis_client = await aioredis.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            encoding="utf8"
        )
    return redis_client


@app.on_startup
async def startup():
    global redis_client, order_consumer

    try:
        redis_client = await aioredis.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            encoding="utf8"
        )
        await redis_client.ping()
        logger.info("Redis connected")

        await seed_test_data(redis_client)

        order_consumer = OrderConsumer(kafka_broker, redis_client)
        logger.info("Order consumer initialized")

    except Exception as e:
        logger.error(f"Failed to initialize: {e}")
        raise


@app.on_shutdown
async def shutdown():
    global redis_client
    if redis_client:
        await redis_client.aclose()
        logger.info("Redis disconnected")


@kafka_broker.subscriber("order.created")
async def handle_order_created(message: OrderCreatedEvent):
    global order_consumer
    if order_consumer:
        await order_consumer.process_order_created(message)


@kafka_broker.subscriber("order.completed")
async def handle_order_completed(message: OrderCompletedEvent):
    order_id = message.order_id
    logger.info(f"Order completed: {order_id}")


@kafka_broker.subscriber("order.failed")
async def handle_order_failed(message: OrderFailedEvent):
    order_id = message.order_id
    reason = message.reason
    logger.warning(f"Order failed: {order_id} - {reason}")


@kafka_broker.subscriber("order.cancelled")
async def handle_order_cancelled(message: OrderCancelledEvent):
    order_id = message.order_id
    reason = message.reason
    logger.info(f"Order cancelled: {order_id} - {reason}")


async def create_order_example():
    redis = await get_redis()
    producer = OrderProducer(kafka_broker, redis)

    order = await producer.create_order(
        user_id="user_1",
        items=[
            {"product_id": "P1", "quantity": 1, "price": 999.99},
            {"product_id": "P2", "quantity": 2, "price": 29.99}
        ]
    )

    logger.info(f"Created order: {order.order_id}")
    return order