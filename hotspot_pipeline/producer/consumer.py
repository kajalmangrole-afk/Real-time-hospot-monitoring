import os
import json
import uuid

from kafka_client import get_consumer

TOPICS = [
    topic.strip()
    for topic in os.getenv("KAFKA_TOPICS", os.getenv("KAFKA_TOPIC_DNS", "dns_activity_events")).split(",")
    if topic.strip()
]
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", f"hotspot-consumer-{uuid.uuid4().hex}")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

consumer = get_consumer(
    TOPICS,
    group_id=GROUP_ID,
    auto_offset_reset=AUTO_OFFSET_RESET,
)

print(f"Consumer started for topics={TOPICS} group_id={GROUP_ID} offset_reset={AUTO_OFFSET_RESET}")

for message in consumer:
    print("\nTopic:", message.topic)
    print("Partition:", message.partition, "Offset:", message.offset)
    print("Received:", message.value)
