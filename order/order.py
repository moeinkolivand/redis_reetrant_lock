import asyncio
from typing import List

import redis.asyncio as aioredis
from redis_module.redis_reentrant_lock import RedisReentrantLock, reentrant_lock
import json
from datetime import datetime
from enum import Enum
import logging

from redis_module.redis_seeder import seed_test_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderStatus(str, Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    PAYMENT_PROCESSING = "payment_processing"
    PAYMENT_COMPLETED = "payment_completed"
    INVENTORY_RESERVED = "inventory_reserved"
    SHIPPING_SCHEDULED = "shipping_scheduled"
    COMPLETED = "completed"
    FAILED = "failed"


class PaymentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class InventoryStatus(str, Enum):
    AVAILABLE = "available"
    RESERVED = "reserved"
    ALLOCATED = "allocated"
    OUT_OF_STOCK = "out_of_stock"


class ShippingStatus(str, Enum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"


class Order:
    """Order entity with nested transaction data."""

    def __init__(self, order_id: str, user_id: str, items: List[dict]):
        self.order_id = order_id
        self.user_id = user_id
        self.items = items
        self.status = OrderStatus.PENDING
        self.payment_info = {
            "status": PaymentStatus.PENDING,
            "amount": sum(item["quantity"] * item["price"] for item in items),
            "payment_method": None,
            "transaction_id": None
        }
        self.inventory_info = {
            "status": InventoryStatus.AVAILABLE,
            "reserved_items": []
        }
        self.shipping_info = {
            "status": ShippingStatus.PENDING,
            "tracking_number": None,
            "estimated_delivery": None
        }
        self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()
        self.transaction_log = []

    def add_log(self, message: str, lock_count: int = None):
        """Add transaction log entry."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "message": message
        }
        if lock_count:
            entry["lock_count"] = lock_count
        self.transaction_log.append(entry)

    def to_dict(self) -> dict:
        """Convert to dictionary for Redis storage."""
        return {
            "order_id": self.order_id,
            "user_id": self.user_id,
            "items": self.items,
            "status": self.status,
            "payment_info": self.payment_info,
            "inventory_info": self.inventory_info,
            "shipping_info": self.shipping_info,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "transaction_log": self.transaction_log
        }


class OrderProcessingService:
    """Service for processing orders with nested transactions using reentrant locks."""

    def __init__(self, redis: aioredis.Redis):
        self.redis = redis
        self.lock = RedisReentrantLock(redis, ttl_ms=60000)

    async def save_order(self, order: Order):
        """Save order to Redis."""
        order.updated_at = datetime.now().isoformat()
        order_key = f"order:{order.order_id}"
        await self.redis.set(order_key, json.dumps(order.to_dict()))
        logger.debug(f"Saved order {order.order_id} to Redis")

    async def get_order(self, order_id: str) -> Order:
        """Get order from Redis."""
        order_key = f"order:{order_id}"
        order_data = await self.redis.get(order_key)
        if not order_data:
            raise ValueError(f"Order {order_id} not found")

        data = json.loads(order_data)
        order = Order(data["order_id"], data["user_id"], data["items"])
        order.status = data["status"]
        order.payment_info = data["payment_info"]
        order.inventory_info = data["inventory_info"]
        order.shipping_info = data["shipping_info"]
        order.created_at = data["created_at"]
        order.updated_at = data["updated_at"]
        order.transaction_log = data["transaction_log"]
        return order

    async def get_lock_count(self, order_id: str, owner_id: str) -> int:
        """Get current lock acquisition count."""
        return await self.lock._get_acquisition_count(f"order:{order_id}", owner_id)

    async def process_order(self, order_id: str, owner_id: str) -> bool:
        """Main order processing entry point. Lock Level: 1"""
        logger.info(f"{owner_id} STARTING ORDER PROCESSING: {order_id}")

        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            logger.error(f"{owner_id} Failed to acquire lock for order {order_id}")
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock acquired [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.add_log(f"Processing started by {owner_id}", lock_count)

            logger.info(f"{owner_id} Step 1: Validating order...")
            if not await self.validate_order(order_id, owner_id):
                order.status = OrderStatus.FAILED
                await self.save_order(order)
                return False

            await asyncio.sleep(0.5)

            logger.info(f"{owner_id} Step 2: Processing payment...")
            if not await self.process_payment(order_id, owner_id, "credit_card"):
                order.status = OrderStatus.FAILED
                await self.save_order(order)
                return False

            await asyncio.sleep(0.5)

            logger.info(f"{owner_id} Step 3: Updating inventory...")
            if not await self.update_inventory(order_id, owner_id):
                order.status = OrderStatus.FAILED
                await self.save_order(order)
                return False

            await asyncio.sleep(0.5)

            logger.info(f"{owner_id} Step 4: Scheduling shipping...")
            if not await self.schedule_shipping(order_id, owner_id):
                order.status = OrderStatus.FAILED
                await self.save_order(order)
                return False

            await self.finalize_order(order_id, owner_id)

            logger.info(f"\n{owner_id} ORDER PROCESSING COMPLETED SUCCESSFULLY")
            return True

        except Exception as e:
            logger.error(f"{owner_id} Error processing order: {e}")
            order = await self.get_order(order_id)
            order.status = OrderStatus.FAILED
            order.add_log(f"Error: {str(e)}")
            await self.save_order(order)
            return False

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Main lock released [Count: {lock_count}]\n")

    async def validate_order(self, order_id: str, owner_id: str) -> bool:
        """Validate order. Lock Level: Uses existing lock from process_order"""
        lock_count = await self.get_lock_count(order_id, owner_id)
        logger.info(f"{owner_id} Validating order [Lock Count: {lock_count}]")

        order = await self.get_order(order_id)

        if not order.items:
            logger.error(f"{owner_id} Order has no items")
            return False

        user_key = f"user:{order.user_id}:json"
        user_exists = await self.redis.exists(user_key)
        if not user_exists:
            logger.error(f"{owner_id} User {order.user_id} not found")
            return False

        order.status = OrderStatus.VALIDATED
        order.add_log("Order validated", lock_count)
        await self.save_order(order)

        logger.info(f"{owner_id} Order validated successfully")
        return True

    async def process_payment(self, order_id: str, owner_id: str, payment_method: str) -> bool:
        """Process payment. Lock Level: 2 (REENTRY)"""
        logger.info(f"{owner_id} PAYMENT PROCESSING MODULE")

        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            logger.error(f"{owner_id} Failed to acquire lock for payment")
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.status = OrderStatus.PAYMENT_PROCESSING
            order.payment_info["status"] = PaymentStatus.PROCESSING
            order.payment_info["payment_method"] = payment_method
            order.add_log(f"Payment processing started with {payment_method}", lock_count)
            await self.save_order(order)

            await asyncio.sleep(1.0)

            logger.info(f"{owner_id} Updating payment status...")
            await self.update_order_payment_status(order_id, owner_id, "TXN-123456")

            await self.verify_payment(order_id, owner_id)

            logger.info(f"{owner_id} Payment processed successfully")
            return True

        except Exception as e:
            logger.error(f"{owner_id} Payment failed: {e}")
            order = await self.get_order(order_id)
            order.payment_info["status"] = PaymentStatus.FAILED
            await self.save_order(order)
            return False

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Payment module lock released [Count: {lock_count}]")

    async def update_order_payment_status(self, order_id: str, owner_id: str, transaction_id: str) -> bool:
        """Update payment status. Lock Level: 3 (DEEPER REENTRY)"""
        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (DEEPER REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.payment_info["transaction_id"] = transaction_id
            order.payment_info["status"] = PaymentStatus.COMPLETED
            order.status = OrderStatus.PAYMENT_COMPLETED
            order.add_log(f"Payment completed: {transaction_id}", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} Payment status updated")
            return True

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Payment status update released [Count: {lock_count}]")

    async def verify_payment(self, order_id: str, owner_id: str) -> bool:
        """Verify payment was successful."""
        lock_count = await self.get_lock_count(order_id, owner_id)
        logger.info(f"{owner_id} Verifying payment [Lock Count: {lock_count}]")

        order = await self.get_order(order_id)
        if order.payment_info["status"] != PaymentStatus.COMPLETED:
            return False

        logger.info(f"{owner_id} Payment verified")
        return True

    async def update_inventory(self, order_id: str, owner_id: str) -> bool:
        """Update inventory. Lock Level: 2 (REENTRY)"""
        logger.info(f"{owner_id} INVENTORY MANAGEMENT MODULE")

        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            logger.error(f"{owner_id} Failed to acquire lock for inventory")
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.add_log("Inventory update started", lock_count)

            logger.info(f"{owner_id} Reserving items...")
            if not await self.reserve_items(order_id, owner_id):
                return False

            await asyncio.sleep(0.5)

            logger.info(f"{owner_id} Updating items status...")
            await self.update_order_items_status(order_id, owner_id)

            order = await self.get_order(order_id)
            order.inventory_info["status"] = InventoryStatus.ALLOCATED
            order.status = OrderStatus.INVENTORY_RESERVED
            order.add_log("Inventory allocated", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} Inventory updated successfully")
            return True

        except Exception as e:
            logger.error(f"{owner_id} Inventory update failed: {e}")
            return False

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Inventory module lock released [Count: {lock_count}]")

    async def reserve_items(self, order_id: str, owner_id: str) -> bool:
        """Reserve items. Lock Level: 3 (DEEPER REENTRY)"""
        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (DEEPER REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            reserved_items = []

            for item in order.items:
                product_key = f"product:{item['product_id']}"
                stock = await self.redis.get(product_key)

                if stock and int(stock) >= item["quantity"]:
                    await self.redis.decrby(product_key, item["quantity"])
                    reserved_items.append({
                        "product_id": item["product_id"],
                        "quantity": item["quantity"],
                        "reserved_at": datetime.now().isoformat()
                    })
                    logger.info(f"{owner_id} Reserved {item['quantity']}x {item['product_id']}")
                else:
                    logger.error(f"{owner_id} Insufficient stock for {item['product_id']}")
                    return False

            order.inventory_info["reserved_items"] = reserved_items
            order.inventory_info["status"] = InventoryStatus.RESERVED
            order.add_log(f"Reserved {len(reserved_items)} items", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} All items reserved")
            return True

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Item reservation released [Count: {lock_count}]")

    async def update_order_items_status(self, order_id: str, owner_id: str):
        """Update items status. Lock Level: 3 (DEEPER REENTRY)"""
        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (DEEPER REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)

            for item in order.items:
                item["status"] = "allocated"
                item["allocated_at"] = datetime.now().isoformat()

            order.add_log("Item statuses updated", lock_count)
            await self.save_order(order)

            logger.info(f"[{owner_id}] Items status updated")
            return True

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"[{owner_id}] Items status update released [Count: {lock_count}]")

    async def schedule_shipping(self, order_id: str, owner_id: str) -> bool:
        """Schedule shipping. Lock Level: 2 (REENTRY)"""
        logger.info(f"{owner_id} SHIPPING MANAGEMENT MODULE")

        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            logger.error(f"{owner_id} Failed to acquire lock for shipping")
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.add_log("Shipping scheduling started", lock_count)

            logger.info(f"{owner_id} Creating shipment...")
            tracking_number = await self.create_shipment(order_id, owner_id)

            await asyncio.sleep(0.5)

            logger.info(f"{owner_id} Updating shipping info...")
            await self.update_order_shipping_info(order_id, owner_id, tracking_number)

            order = await self.get_order(order_id)
            order.shipping_info["status"] = ShippingStatus.SCHEDULED
            order.status = OrderStatus.SHIPPING_SCHEDULED
            order.add_log("Shipping scheduled", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} Shipping scheduled successfully")
            return True

        except Exception as e:
            logger.error(f"{owner_id} Shipping scheduling failed: {e}")
            return False

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Shipping module lock released [Count: {lock_count}]")

    async def create_shipment(self, order_id: str, owner_id: str) -> str:
        """Create shipment. Lock Level: 3 (DEEPER REENTRY)"""
        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            return None

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (DEEPER REENTRY) [Count: {lock_count}]")

            import random
            tracking_number = f"TRACK-{random.randint(100000, 999999)}"

            order = await self.get_order(order_id)
            order.add_log(f"Shipment created: {tracking_number}", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} Shipment created: {tracking_number}")
            return tracking_number

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Shipment creation released [Count: {lock_count}]")

    async def update_order_shipping_info(self, order_id: str, owner_id: str, tracking_number: str):
        """Update shipping info. Lock Level: 3 (DEEPER REENTRY)"""
        lock_acquired = await self.lock.acquire(f"order:{order_id}", owner_id)
        if not lock_acquired:
            return False

        try:
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Lock re-acquired (DEEPER REENTRY) [Count: {lock_count}]")

            order = await self.get_order(order_id)
            order.shipping_info["tracking_number"] = tracking_number
            order.shipping_info["estimated_delivery"] = "2024-12-20"
            order.add_log(f"Shipping info updated: {tracking_number}", lock_count)
            await self.save_order(order)

            logger.info(f"{owner_id} Shipping info updated")
            return True

        finally:
            await self.lock.release(f"order:{order_id}", owner_id)
            lock_count = await self.get_lock_count(order_id, owner_id)
            logger.info(f"{owner_id} Shipping info update released [Count: {lock_count}]")

    async def finalize_order(self, order_id: str, owner_id: str):
        """Finalize order processing."""
        lock_count = await self.get_lock_count(order_id, owner_id)
        logger.info(f"\n{owner_id} Finalizing order [Lock Count: {lock_count}]")

        order = await self.get_order(order_id)
        order.status = OrderStatus.COMPLETED
        order.add_log("Order completed successfully", lock_count)
        await self.save_order(order)

        logger.info(f"{owner_id} Order finalized")



async def demonstrate_nested_transaction_processing():
    """Demonstrate reentrant lock with nested transaction processing."""

    redis = await aioredis.from_url(
        "redis://localhost:6379/0",
        encoding="utf-8",
        decode_responses=True
    )

    try:
        logger.info("REENTRANT LOCK DEMONSTRATION")
        logger.info("Scenario: Nested Transaction Processing (Order → Payment → Inventory → Shipping)")

        await seed_test_data(redis)

        service = OrderProcessingService(redis)

        order = Order(
            order_id="ORD-2024-001",
            user_id="user_1",
            items=[
                {"product_id": "P1", "quantity": 1, "price": 999.99},
                {"product_id": "P2", "quantity": 2, "price": 29.99}
            ]
        )
        await service.save_order(order)
        logger.info(f"✓ Order {order.order_id} created\n")

        owner_id = "transaction-worker-001"
        success = await service.process_order(order.order_id, owner_id)

        if success:
            logger.info(" DEMONSTRATION COMPLETED SUCCESSFULLY!")
        else:
            logger.error("\n❌ Order processing failed")

    finally:
        await redis.aclose()
