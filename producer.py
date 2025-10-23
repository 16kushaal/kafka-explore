from confluent_kafka import Producer
import uuid
import json

producer_config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for Order {msg.key()}: {err}")
    else:
        print(f"Order {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

        
order = {
    "order_id": str(uuid.uuid4()),
    "user": "kushaa16",
    "item": "mushroom pizza",
    "quantity": 2
}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic="orders", 
    value=value,
    callback=delivery_report
    )

producer.flush()