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
    produce(bootstrap, topic)
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
    deadline = time.time() + 90
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


def read(path):
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.startswith("v")}


if __name__ == "__main__":
    main()
