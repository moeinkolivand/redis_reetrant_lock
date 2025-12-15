import json
from typing import List, Optional

from faststream.kafka import KafkaMessage
from pydantic import BaseModel
import redis.asyncio as aioredis
from faststream.kafka import KafkaBroker
from faststream import context
import logging

from order.order import OrderProcessingService

logger = logging.getLogger(__name__)


class OrderCreatedEvent(BaseModel):
    event_type: str
    order_id: str
    user_id: str
    items: List[dict]
    total_amount: float
    status: str
    timestamp: str


class OrderCompletedEvent(BaseModel):
    event_type: str = "order.completed"
    order_id: str
    status: str
    payment_transaction_id: Optional[str]
    tracking_number: Optional[str]
    timestamp: str


class OrderFailedEvent(BaseModel):
    event_type: str = "order.failed"
    order_id: str
    reason: str
    timestamp: str


class OrderConsumer:

    def __init__(self, broker: KafkaBroker, redis: aioredis.Redis):
        self.broker = broker
        self.redis = redis
        self.service = OrderProcessingService(redis)
        self.topic = "order.created"
        self.completed_topic = "order.completed"
        self.failed_topic = "order.failed"

    async def process_order_created(self, message: OrderCreatedEvent, kafka_message: KafkaMessage | None = None):
        order_id = message.order_id

        logger.info(f"Processing order.created event for {order_id}")

        partition = kafka_message.raw_message.partition if kafka_message is not None else 0
        owner_id = f"consumer-{partition}"

        try:
            success = await self.service.process_order(order_id, owner_id)

            if success:
                await self._publish_order_completed(order_id)
            else:
                await self._publish_order_failed(order_id, "Processing failed")

        except Exception as e:
            logger.error(f"Error processing order {order_id}: {e}")
            await self._publish_order_failed(order_id, str(e))

    async def _publish_order_completed(self, order_id: str):
        order_key = f"order:{order_id}"
        order_data = await self.redis.get(order_key)

        if order_data:
            data = json.loads(order_data)

            event = OrderCompletedEvent(
                order_id=order_id,
                status=data["status"],
                payment_transaction_id=data["payment_info"].get("transaction_id"),
                tracking_number=data["shipping_info"].get("tracking_number"),
                timestamp=data["updated_at"]
            )

            await self.broker.publish(
                message=event,
                topic=self.completed_topic,
                key=order_id.encode()
            )

            logger.info(f"Published order.completed event for {order_id}")

    async def _publish_order_failed(self, order_id: str, reason: str):
        from datetime import datetime

        event = OrderFailedEvent(
            order_id=order_id,
            reason=reason,
            timestamp=datetime.now().isoformat()
        )

        await self.broker.publish(
            message=event,
            topic=self.failed_topic,
            key=order_id.encode()
        )

        logger.warning(f"Published order.failed event for {order_id}: {reason}")
