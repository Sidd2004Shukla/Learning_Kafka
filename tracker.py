import json
from confluent_kafka import Consumer
consumer_config={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order_tracker',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])
print("Waiting for messages .... ")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Error: {msg.error()}')
        value=msg.value().decode("utf-8")
        order=json.loads(value)
        print(f'Order ID: {order["item"]} * {order["quantity"]} by user: {order["user"]}')
except KeyboardInterrupt:
    print("Shutting down...")
finally:
    consumer.close()


