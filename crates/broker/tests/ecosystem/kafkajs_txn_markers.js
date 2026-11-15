// KafkaJS reading a transactional partition read_committed, for the
// ecosystem harness (ADR 0021). The partition is the one TxnMarkers.java
// wrote. Arguments: bootstrap address, topic, the values to see in order.
const { Kafka, logLevel } = require("kafkajs");

async function main() {
  const [bootstrap, topic, wanted] = process.argv.slice(2);
  const want = wanted.split(",");
  const kafka = new Kafka({ brokers: [bootstrap], logLevel: logLevel.ERROR });
  const consumer = kafka.consumer({
    groupId: `${topic}-kafkajs-${Date.now()}`,
    readUncommitted: false,
  });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const seen = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`saw ${seen.join(",")} within 30s, want ${wanted}`)),
      30000,
    );
    consumer.run({
      eachMessage: async ({ message }) => {
        seen.push(message.value.toString());
        if (seen.length >= want.length) {
          clearTimeout(timer);
          resolve();
        }
      },
    });
  });
  await consumer.disconnect();
  if (seen.join(",") !== want.join(",")) {
    throw new Error(`read_committed saw ${seen.join(",")}, want ${wanted}`);
  }
  console.log("kafkajs txn markers ok");
}

main().catch((err) => {
  console.error("kafkajs txn markers:", err);
  process.exit(1);
});
