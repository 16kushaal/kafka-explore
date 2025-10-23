from confluent_kafka import Producer
import uuid
import json

producer_config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌Delivery failed: {err}")
    else:
        print(f"✅ Delivery successful for Order {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()} and partition [{msg.partition()}] at offset {msg.offset()}")


order = {
    "order_id": str(uuid.uuid4()),
    "user": "evole112",
    "item": "chicken pizza",
    "quantity": 4
}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic="orders", 
    value=value,
    callback=delivery_report
    )

producer.flush()