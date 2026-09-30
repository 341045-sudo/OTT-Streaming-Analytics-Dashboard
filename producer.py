from kafka import KafkaProducer
import csv
import json
import time

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ott.clickstream.raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

print("Connected to Kafka successfully.")
print(f"Publishing messages to topic: {TOPIC_NAME}")
print("-" * 70)

with open("ott_clickstream_data.csv", "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        event = {
            "userId": row["userId"],
            "timestamp": int(row["timestamp"]),
            "eventType": row["eventType"],
            "contentId": row["contentId"],
            "position_sec": int(row["position_sec"])
        }

        future = producer.send(TOPIC_NAME, value=event)
        metadata = future.get(timeout=10)

        print(
            f"Sent: {event} "
            f"| Partition: {metadata.partition} "
            f"| Offset: {metadata.offset}"
        )

        time.sleep(1)

producer.flush()
producer.close()

print("-" * 70)
print("All 50 clickstream messages sent successfully.")
