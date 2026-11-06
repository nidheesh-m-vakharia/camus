"""librdkafka via confluent-kafka: create, produce, and assign-fetch each codec."""

import sys
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

bootstrap, trust = sys.argv[1], sys.argv[2]
common = {
    "bootstrap.servers": bootstrap,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "camus",
    "sasl.password": "camus-secret",
    "ssl.ca.location": trust,
    "socket.timeout.ms": 10000,
}
admin = AdminClient(common)

for codec in ("none", "gzip", "snappy", "lz4", "zstd"):
    topic = "p" + codec + uuid.uuid4().hex[:6]
    value = codec.encode()
    created = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    created[topic].result()
    producer = Producer(
        {
            **common,
            "acks": "all",
            "enable.idempotence": False,
            "compression.type": codec,
        }
    )
    producer.produce(topic, key=b"k", value=value)
    remaining = producer.flush(15)
    if remaining:
        raise SystemExit(f"produce flush left {remaining} for {codec}")
    consumer = Consumer(
        {
            **common,
            "group.id": "py-" + topic,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    found = False
    for _ in range(30):
        record = consumer.poll(0.5)
        if record is None:
            continue
        if record.error():
            raise SystemExit(f"{codec}: {record.error()}")
        if record.value() == value:
            found = True
            break
    consumer.close()
    if not found:
        raise SystemExit(f"missing {codec}")
    print(f"ok {codec}", flush=True)
