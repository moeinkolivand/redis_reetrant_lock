import uuid
import json
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import redis.asyncio as aioredis
from faststream.kafka import KafkaBroker
import logging

from order.order import Order, OrderStatus

logger = logging.getLogger(__name__)


class OrderCreatedEvent(BaseModel):
    event_type: str = Field(default="order.created")
    order_id: str
    user_id: str
    items: list[dict]
    total_amount: float
    status: str
    timestamp: str


class OrderCancelledEvent(BaseModel):
    event_type: str = Field(default="order.cancelled")
    order_id: str
    reason: str
    timestamp: str


class OrderProducer:

    def __init__(self, broker: KafkaBroker, redis: aioredis.Redis):
        self.broker = broker
        self.redis = redis
        self.topic = "order.created"

    async def create_order(
            self,
            user_id: str,
            items: list[dict],
            order_id: Optional[str] = None
    ) -> Order:
        if not order_id:
            order_id = f"ORD-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

        order = Order(order_id=order_id, user_id=user_id, items=items)

        await self._save_order(order)

        await self._publish_order_created_event(order)

        logger.info(f"Order created: {order_id} for user {user_id}")

        return order

    async def _save_order(self, order: Order):
        order_key = f"order:{order.order_id}"
        await self.redis.set(order_key, json.dumps(order.to_dict()))
        logger.debug(f"Saved order {order.order_id} to Redis")

    async def _publish_order_created_event(self, order: Order):
        event = OrderCreatedEvent(
            order_id=order.order_id,
            user_id=order.user_id,
            items=order.items,
            total_amount=order.payment_info["amount"],
            status=order.status,
            timestamp=datetime.now().isoformat()
        )

        await self.broker.publish(
            message=event,
            topic=self.topic,
            key=order.order_id.encode()
        )

        logger.info(f"Published order.created event for {order.order_id}")

    async def cancel_order(self, order_id: str, reason: str = "User requested"):
        order_key = f"order:{order_id}"
        order_data = await self.redis.get(order_key)

        if not order_data:
            raise ValueError(f"Order {order_id} not found")

        data = json.loads(order_data)
        order = Order(data["order_id"], data["user_id"], data["items"])
        order.status = OrderStatus.FAILED
        order.add_log(f"Order cancelled: {reason}")

        await self._save_order(order)

        cancel_event = OrderCancelledEvent(
            order_id=order_id,
            reason=reason,
            timestamp=datetime.now().isoformat()
        )

        await self.broker.publish(
            message=cancel_event,
            topic="order.cancelled",
            key=order_id.encode()
        )

        logger.info(f"Order {order_id} cancelled: {reason}")