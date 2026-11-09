// KafkaJS round trip for the ecosystem harness: create a topic, produce,
// and consume it back in a group. Argument: bootstrap address.
const { Kafka, logLevel } = require("kafkajs");

async function main() {
  const kafka = new Kafka({ brokers: [process.argv[2]], logLevel: logLevel.ERROR });
  const topic = `js-${Date.now()}`;
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  await producer.send({ topic, messages: [{ key: "k", value: "from-kafkajs" }] });
  await producer.disconnect();

  const consumer = kafka.consumer({ groupId: `${topic}-group` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const seen = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("no message within 30s")), 30000);
    consumer.run({
      eachMessage: async ({ message }) => {
        clearTimeout(timer);
        resolve(message.value.toString());
      },
    });
  });
  await consumer.disconnect();
  if (seen !== "from-kafkajs") throw new Error(`read ${seen}`);
  console.log("kafkajs ok");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
