"""librdkafka via confluent-kafka: two consumers share a group, commit, leave;
a third member of the same group resumes from the committed offsets."""

import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

bootstrap = sys.argv[1]
protocol = sys.argv[2] if len(sys.argv) > 2 else "classic"
topic = "g" + uuid.uuid4().hex[:8]
group = "group-" + uuid.uuid4().hex[:8]
common = {"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000}

admin = AdminClient(common)
admin.create_topics([NewTopic(topic, num_partitions=4, replication_factor=3)])[topic].result()


def produce(first, count):
    producer = Producer({**common, "acks": "all", "enable.idempotence": False})
    for index in range(first, first + count):
        producer.produce(topic, key=str(index).encode(), value=f"v{index}".encode())
    assert producer.flush(30) == 0, "unflushed records"


def consumer(name):
    config = {
        **common,
        "group.id": group,
        "client.id": name,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    # The consumer protocol takes its session from the broker. Classic
    # members still send these.
    if protocol == "consumer":
        config["group.protocol"] = "consumer"
    else:
        config["session.timeout.ms"] = 10000
        config["heartbeat.interval.ms"] = 1000
    member = Consumer(config)
    member.subscribe([topic])
    return member


def drain(members, want, seconds):
    """Poll every member until `want` distinct values arrived. A rebalance
    before the first commit may deliver a record twice (at least once), so
    count distinct values."""
    seen = set()
    deadline = time.time() + seconds
    while len(seen) < want:
        assert time.time() < deadline, f"only {len(seen)} of {want}"
        for member in members:
            message = member.poll(0.2)
            if message is None or message.error():
                continue
            seen.add(message.value().decode())
    return seen


produce(0, 40)
first, second = consumer("first"), consumer("second")
seen = drain([first, second], 40, 60)
assert seen == {f"v{index}" for index in range(40)}, sorted(seen)
assert first.assignment() and second.assignment(), "both members hold partitions"
assert len(first.assignment()) + len(second.assignment()) == 4
for member in (first, second):
    member.commit(asynchronous=False)
    member.close()

produce(40, 20)
third = consumer("third")
resumed = drain([third], 20, 60)
assert resumed == {f"v{index}" for index in range(40, 60)}, sorted(resumed)
# Nothing older comes back.
time.sleep(2)
extra = third.poll(1.0)
assert extra is None or extra.error(), f"re-read {extra.value()}"
third.commit(asynchronous=False)
third.close()
listed = admin.list_consumer_groups().result()
assert any(row.group_id == group for row in listed.valid), listed
described = admin.describe_consumer_groups([group])[group].result()
assert described.group_id == group, described
admin.delete_consumer_groups([group])[group].result()
print(f"librdkafka group ok ({protocol}): 40 shared by two members, 20 resumed by a third")
