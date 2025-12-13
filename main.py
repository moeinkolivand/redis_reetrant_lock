import logging
from datetime import datetime
from typing import Optional
import redis.asyncio as aioredis
from aiokafka.admin import NewTopic, AIOKafkaAdminClient
from aiokafka.errors import TopicAlreadyExistsError
from faststream.kafka import KafkaBroker
from faststream import FastStream, Depends


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
KAFKA_BROKERS : list[str] = ["localhost:9092", "localhost:9094", "localhost:9096"]



kafka_broker = KafkaBroker(KAFKA_BROKERS)
app = FastStream(kafka_broker)

redis_client: Optional[aioredis.Redis] = None


TRANSFER_REQUEST_TOPIC = "order.transfer.requested"
TRANSFER_COMPLETED_TOPIC = "order.transfer.completed"


async def create_kafka_topics():
    """Create Kafka topics if they don't exist."""
    try:
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="wallet-app-admin"
        )

        topics = [
            NewTopic(
                name=TRANSFER_REQUEST_TOPIC,
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                name=TRANSFER_COMPLETED_TOPIC,
                num_partitions=3,
                replication_factor=1
            )
        ]

        fs = admin_client.create_topics(new_topics=topics, validate_only=False)

        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic '{topic}' created successfully")
            except TopicAlreadyExistsError:
                logger.info(f"Topic '{topic}' already exists")
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {e}")

        await admin_client.close()

    except Exception as e:
        logger.error(f"Failed to initialize Kafka admin client: {e}")


async def get_redis() -> aioredis.Redis:
    """
    Dependency provider for Redis instance.
    Returns the global redis_client (initialized in startup).
    """
    global redis_client
    if redis_client is None:
        return aioredis.from_url("redis://localhost:6379/1", decode_responses=True)
    return redis_client


@app.on_startup
async def startup():
    """Initialize Redis on application startup."""
    global redis_client

    try:
        redis_client = aioredis.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            encoding="utf8"
        )
        await redis_client.ping()
        logger.info("✓ Redis connected")
        await seed_redis()

    except Exception as e:
        logger.error(f"❌ Failed to initialize Redis: {e}")
        raise


@app.on_shutdown
async def shutdown():
    """Cleanup Redis connection on shutdown."""
    global redis_client
    if redis_client:
        await redis_client.close()
        logger.info("Redis disconnected")

