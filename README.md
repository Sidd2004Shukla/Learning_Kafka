# Kafka Order Tracking Example

This project demonstrates a simple **Apache Kafka Producer-Consumer workflow** using Python and the `confluent-kafka` library.

## Workflow

```text
Producer (producer.py)
        |
        v
   Kafka Topic
    "orders"
        |
        v
Consumer (tracker.py)
```

The producer publishes an order event to the `orders` topic, and the consumer listens for new messages and processes them in real time.

---

## Producer

The producer creates an order event and sends it to the Kafka topic.

### Features

- Generates a unique Order ID using UUID.
- Serializes data using JSON.
- Publishes events to Kafka.
- Provides delivery status via callback.

### Sample Order Event

```json
{
  "order_id": "77e24fab-cf65-414e-881e-a9e5150b49ce",
  "user": "siddharth shukla",
  "item": "pizza makhani",
  "quantity": 1
}
```

### Producer Code

```python
import json
import uuid
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

order = {
    "order_id": str(uuid.uuid4()),
    "user": "siddharth shukla",
    "item": "pizza makhani",
    "quantity": 1,
}

def delivery_logs(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.value().decode('utf-8')}")
        print(
            f"Topic: {msg.topic()} "
            f"Partition: {msg.partition()} "
            f"Offset: {msg.offset()}"
        )

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_logs
)

producer.flush()
```

---

## Consumer

The consumer subscribes to the `orders` topic and processes incoming messages.

### Features

- Subscribes to Kafka topics.
- Reads messages from the earliest offset.
- Deserializes JSON messages.
- Displays order details in real time.

### Consumer Code

```python
import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order_tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)

        print(
            f'Order: {order["item"]} x {order["quantity"]} '
            f'by user {order["user"]}'
        )

except KeyboardInterrupt:
    print("Shutting down...")

finally:
    consumer.close()
```

---

## Running the Project

### 1. Start Kafka

```bash
docker compose up -d
```

### 2. Run Consumer

```bash
python tracker.py
```

Expected Output:

```text
Waiting for messages...
```

### 3. Run Producer

```bash
python producer.py
```

Expected Output:

```text
Message delivered to {"order_id":"...","user":"siddharth shukla","item":"pizza makhani","quantity":1}
Topic: orders Partition: 0 Offset: 0
```

### 4. Consumer Receives Message

```text
Order: pizza makhani x 1 by user siddharth shukla
```

---

## Concepts Covered

- Apache Kafka
- Kafka Topics
- Kafka Producers
- Kafka Consumers
- Consumer Groups
- Event-Driven Architecture
- JSON Serialization
- Message Delivery Callbacks
- Real-Time Event Processing

---

## Future Improvements

- Add multiple partitions
- Implement consumer groups
- Add retry mechanisms
- Integrate PostgreSQL
- Integrate Redis
- Build an order analytics dashboard
- Implement dead-letter queues (DLQ)
- Create a complete event-driven microservice architecture
