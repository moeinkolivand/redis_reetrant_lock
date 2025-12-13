import aioredis


async def seed_test_data(redis: aioredis.Redis):
    """Seed test data for the example."""
    print("Seeding test data...")

    user_data = {
        "user_id": "user_1",
        "full_name": "John Smith",
        "email": "john@example.com",
        "status": "active"
    }
    await redis.set("user:user_1:json", json.dumps(user_data))

    products = {
        "P1": {"name": "Laptop", "stock": 50, "price": 999.99},
        "P2": {"name": "Mouse", "stock": 100, "price": 29.99},
        "P3": {"name": "Keyboard", "stock": 75, "price": 79.99}
    }

    for product_id, product_data in products.items():
        await redis.set(f"product:{product_id}", product_data["stock"])
        await redis.set(f"product:{product_id}:info", json.dumps(product_data))

    print("Test data seeded")
