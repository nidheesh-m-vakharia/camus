"""Two librdkafka members read without committing. One is SIGKILLed.
The survivor must receive that member's records. Classic, then KIP-848."""

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

COUNT = 40


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "member":
        member()
        return
    bootstrap = sys.argv[1]
    for protocol in ("classic", "consumer"):
        run(bootstrap, protocol)
    print("librdkafka kill ok: uncommitted records redelivered, classic and consumer")


def run(bootstrap, protocol):
    topic = "g" + uuid.uuid4().hex[:8]
    group = "kill-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, num_partitions=4, replication_factor=3)])[topic].result()
    directory = Path(os.environ.get("TMPDIR", "/tmp")) / f"group-kill-{uuid.uuid4().hex[:8]}"
    directory.mkdir()
    victim_seen = directory / "victim"
    survivor_seen = directory / "survivor"
    marker = directory / "killed"
    need = directory / "need"
    victim_seen.write_text("")
    survivor_seen.write_text("")
    victim = spawn(bootstrap, topic, group, "victim", protocol, victim_seen, marker, need)
    survivor = spawn(bootstrap, topic, group, "survivor", protocol, survivor_seen, marker, need)
    # Produce only after the two members own disjoint partitions. Otherwise
    # the first member is assigned the whole topic and reads every record
    # before the second joins, and there is nothing left to redeliver.
    assign_deadline = time.time() + 45
    victim_parts = victim_seen.with_name(victim_seen.name + ".parts")
    survivor_parts = survivor_seen.with_name(survivor_seen.name + ".parts")
    while time.time() < assign_deadline and not partitions_split(victim_parts, survivor_parts):
        time.sleep(0.2)
    assert partitions_split(victim_parts, survivor_parts), (
        f"{protocol}: members never took disjoint partitions"
    )
    produce(bootstrap, topic)
    deadline = time.time() + 45
    snapshot = None
    while time.time() < deadline:
        victim_values = read(victim_seen)
        survivor_values = read(survivor_seen)
        if victim_values and survivor_values and not victim_values <= survivor_values:
            snapshot = victim_values
            break
        time.sleep(0.2)
    assert snapshot, f"{protocol}: members never split the log"
    victim.kill()
    victim.wait(timeout=10)
    need.write_text("\n".join(sorted(snapshot)) + "\n")
    marker.write_text("killed\n")
    code = survivor.wait(timeout=45)
    assert code == 0, f"{protocol}: survivor exited {code}"


def spawn(bootstrap, topic, group, role, protocol, seen, marker, need):
    return subprocess.Popen(
        [
            sys.executable,
            __file__,
            "member",
            bootstrap,
            topic,
            group,
            role,
            protocol,
            str(seen),
            str(marker),
            str(need),
        ]
    )


def member():
    _, _, bootstrap, topic, group, role, protocol, seen, marker, need = sys.argv
    config = {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "client.id": role,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "socket.timeout.ms": 10000,
    }
    if protocol == "consumer":
        config["group.protocol"] = "consumer"
    else:
        config["session.timeout.ms"] = 6000
        config["heartbeat.interval.ms"] = 1000
    consumer = Consumer(config)
    consumer.subscribe([topic])
    got = set()
    deadline = time.time() + 150
    path = Path(seen)
    try:
        while time.time() < deadline:
            message = consumer.poll(0.2)
            if message is not None and not message.error():
                value = message.value().decode()
                if value not in got:
                    got.add(value)
                    with path.open("a") as out:
                        out.write(value + "\n")
                        out.flush()
            assigned = consumer.assignment()
            if assigned:
                parts = ",".join(
                    str(tp.partition) for tp in sorted(assigned, key=lambda tp: tp.partition)
                )
                path.with_name(path.name + ".parts").write_text(parts)
            if role == "survivor" and Path(marker).exists() and got == produced():
                return
    finally:
        consumer.close()
    raise AssertionError(f"{role} {protocol} timed out with {len(got)}")


def produce(bootstrap, topic):
    producer = Producer(
        {"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": False}
    )
    for index in range(COUNT):
        producer.produce(
            topic,
            key=str(index).encode(),
            value=f"v{index}".encode(),
            partition=index % 4,
        )
    assert producer.flush(30) == 0


def produced():
    return {f"v{index}" for index in range(COUNT)}


def part_set(path):
    if not path.exists():
        return set()
    text = path.read_text().strip()
    if not text:
        return set()
    return set(text.split(","))


def partitions_split(victim, survivor):
    left, right = part_set(victim), part_set(survivor)
    return bool(left) and bool(right) and left.isdisjoint(right) and len(left | right) == 4


def read(path):
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.startswith("v")}


if __name__ == "__main__":
    main()
