"""librdkafka: a transaction, a compacted log, or an ACL denial."""

import sys
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import (
    AclBinding,
    AclOperation,
    AclPermissionType,
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourcePatternType,
    ResourceType,
)

bootstrap = sys.argv[2]
common = {"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000}
mode = sys.argv[1]


def admin():
    return AdminClient(common)


def create_topic(topic, config=None):
    client = admin()
    deadline = time.time() + 20
    last = None
    while time.time() < deadline:
        try:
            client.create_topics([
                NewTopic(topic, num_partitions=1, replication_factor=1, config=config or {})
            ])[topic].result()
            return client
        except Exception as err:
            last = err
            time.sleep(0.2)
    raise last


def produce_plain(topic, records):
    producer = Producer({**common, "acks": "all", "enable.idempotence": False})
    for key, value in records:
        producer.produce(topic, key=key.encode(), value=value.encode())
    assert producer.flush(15) == 0


def assigned_consumer(topic, **extra):
    member = Consumer({**common, "group.id": "c" + uuid.uuid4().hex[:8], "enable.auto.commit": False, **extra})
    deadline = time.time() + 15
    member.subscribe([topic])
    while not member.assignment():
        assert time.time() < deadline, "assignment did not settle"
        member.poll(0.2)
    return member


def transaction():
    topic = "t" + uuid.uuid4().hex[:8]
    group = "g" + uuid.uuid4().hex[:8]
    create_topic(topic)
    producer = Producer({
        **common,
        "transactional.id": "txn-" + uuid.uuid4().hex[:8],
        "transaction.timeout.ms": 10000,
        "acks": "all",
    })
    producer.init_transactions(15)
    member = Consumer({
        **common,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    member.subscribe([topic])
    deadline = time.time() + 15
    while not member.assignment():
        assert time.time() < deadline, "assignment did not settle"
        member.poll(0.2)
    producer.begin_transaction()
    producer.produce(topic, key=b"k", value=b"open")
    assert producer.flush(15) == 0
    hidden = member.poll(2.0)
    assert hidden is None or hidden.error(), f"saw an open transaction {hidden}"
    producer.send_offsets_to_transaction([TopicPartition(topic, 0, 1)], member.consumer_group_metadata(), 15)
    producer.commit_transaction(15)
    seen = set()
    deadline = time.time() + 15
    while "open" not in seen:
        assert time.time() < deadline, "committed record missing"
        message = member.poll(0.2)
        if message is not None and not message.error():
            seen.add(message.value().decode())
    member.close()
    resumed = Consumer({
        **common,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    resumed.subscribe([topic])
    again = resumed.poll(3.0)
    assert again is None or again.error(), f"offset commit did not hide the record {again}"
    producer.begin_transaction()
    producer.produce(topic, key=b"k", value=b"aborted")
    assert producer.flush(15) == 0
    producer.abort_transaction(15)
    aborted = resumed.poll(2.0)
    assert aborted is None or aborted.error(), f"saw an abort {aborted}"
    producer.begin_transaction()
    producer.produce(topic, key=b"k", value=b"after")
    assert producer.flush(15) == 0
    producer.commit_transaction(15)
    values = set()
    deadline = time.time() + 15
    while "after" not in values:
        assert time.time() < deadline, f"after abort: {values}"
        message = resumed.poll(0.2)
        if message is not None and not message.error():
            values.add(message.value().decode())
    assert values == {"after"}, values
    resumed.close()
    print("librdkafka transaction ok")


def compact():
    topic = "c" + uuid.uuid4().hex[:8]
    client = create_topic(topic, {"durability.mode": "r2"})
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    resource.add_incremental_config(ConfigEntry("cleanup.policy", "compact", incremental_operation=AlterConfigOpType.SET))
    client.incremental_alter_configs([resource])[resource].result()
    produce_plain(topic, [("a", "1"), ("b", "1"), ("a", "2")])
    member = Consumer({**common, "group.id": "c" + uuid.uuid4().hex[:8], "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    deadline = time.time() + 20
    seen = {}
    while seen != {"a": "2", "b": "1"}:
        assert time.time() < deadline, f"compacted {seen}"
        member.subscribe([topic])
        member.poll(0.2)
        for part in member.assignment():
            member.seek(TopicPartition(part.topic, part.partition, 0))
        slice_end = time.time() + 1
        seen = {}
        while time.time() < slice_end:
            message = member.poll(0.2)
            if message is None or message.error():
                continue
            seen[message.key().decode()] = message.value().decode()
    member.close()
    print("librdkafka compaction ok")


def acl():
    topic = "a" + uuid.uuid4().hex[:8]
    client = create_topic(topic)
    produce_plain(topic, [("k", "1")])

    def binding(operation, permission):
        return AclBinding(ResourceType.TOPIC, topic, ResourcePatternType.LITERAL, "User:ANONYMOUS", "*", operation, permission)

    for future in client.create_acls([binding(AclOperation.WRITE, AclPermissionType.ALLOW)]).values():
        future.result()
    produce_plain(topic, [("k", "2")])
    member = assigned_consumer(topic, **{"auto.offset.reset": "earliest"})
    read = member.poll(2.0)
    assert read is not None and read.error(), f"fetch was allowed with only WRITE {read}"
    member.close()
    futures = client.create_acls([
        binding(AclOperation.READ, AclPermissionType.ALLOW),
        binding(AclOperation.WRITE, AclPermissionType.DENY),
    ])
    for future in futures.values():
        future.result()
    described = client.describe_acls(AclBindingFilter_any()).result()
    assert len(described) >= 3, described
    member = assigned_consumer(topic, **{"auto.offset.reset": "earliest"})
    got = None
    deadline = time.time() + 10
    while got is None:
        assert time.time() < deadline, "READ allow did not fetch"
        message = member.poll(0.2)
        if message is not None and not message.error():
            got = message.value()
    member.close()
    errors = []
    producer = Producer({**common, "acks": "all", "enable.idempotence": False})
    producer.produce(topic, key=b"k", value=b"3", on_delivery=lambda err, _msg: errors.append(err))
    producer.flush(15)
    assert errors and errors[0] is not None, "DENY WRITE still produced"
    print("librdkafka acl ok")


def AclBindingFilter_any():
    from confluent_kafka.admin import AclBindingFilter
    return AclBindingFilter(
        ResourceType.ANY, None, ResourcePatternType.ANY, None, None, AclOperation.ANY, AclPermissionType.ANY
    )


if mode == "txn":
    transaction()
elif mode == "compact":
    compact()
elif mode == "acl":
    acl()
else:
    raise SystemExit(mode)
