from confluent_kafka import Consumer
import json

consumer_config= {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumers',
    'auto.offset.reset': 'earliest' #tells the consumer what to do if it cant find where it left off
}

consumer = Consumer(consumer_config)

consumer.subscriber(['orders'])
print("🟢 Consumer is running and subsribed to to orders")

while True:
    msg = consumer.poll(1.0) #polls: asks if there is a new event, the consumer is a pull archi, kafka isnt push archi
    if msg is None:
        continue
    if msg.error():
        print("🔴 Error: ", msg.error())
        continue
    
    value = msg.value().decode('utf-8')
    order = json.loads(value)
    print(f"📦 Received Order: {order['item']} x {order['quantity']} from {order['user']}")
