import json
import uuid
from confluent_kafka import Producer
producer_config={
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

order={
    "order_id":str(uuid.uuid4()),
    "user":"siddharth shukla",
    "item":"pizza makhani",
    "quantity":1,
}
def delivery_logs(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.value().decode("utf-8")}')
        print(f'Message ID: {msg.topic()} Partition: {msg.partition()} Offset: {msg.offset()}')
value=json.dumps(order).encode("utf-8")
producer.produce(
    topic="orders",
    value=value,
    callback=delivery_logs
)
producer.flush()