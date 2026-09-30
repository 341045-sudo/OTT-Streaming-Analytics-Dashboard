from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# ---------------------------
# KAFKA CONFIGURATION
# ---------------------------

TOPIC_NAME = "ott.clickstream.raw"
KAFKA_SERVER = "localhost:9092"

# ---------------------------
# MONGODB CONFIGURATION
# ---------------------------

MONGO_URI = "mongodb+srv://sda_dashboard:kolkata700152@sda-cluster.f09jsry.mongodb.net/?appName=SDA-Cluster"

client = MongoClient(MONGO_URI)

db = client["ott_streaming"]
collection = db["clickstream_events"]

# ---------------------------
# CREATE KAFKA CONSUMER
# ---------------------------

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ott-dashboard-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka consumer started...")
print("Listening to topic:", TOPIC_NAME)

# ---------------------------
# READ KAFKA EVENTS
# AND STORE IN MONGODB
# ---------------------------

for message in consumer:

    event = message.value

    print("Received from Kafka:", event)

    collection.insert_one(event)

    print("Stored in MongoDB successfully.")
