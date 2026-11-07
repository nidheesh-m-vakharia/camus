// kafka-clients 4.4.0: a transactional produce and its offset commit appear
// together for a read_committed consumer. An abort stays hidden.
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public class TxnRoundTrip {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = "t" + UUID.randomUUID().toString().substring(0, 8);
        String group = "g" + UUID.randomUUID().toString().substring(0, 8);
        String transactionalId = "txn-" + UUID.randomUUID().toString().substring(0, 8);

        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        admin.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        admin.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
        producer.initTransactions();

        try (KafkaConsumer<String, String> first = consumer(bootstrap, group)) {
            first.subscribe(List.of(topic));
            awaitAssignment(first);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k", "open")).get();
            check(drain(first, Duration.ofSeconds(2)).isEmpty(), "read_committed saw an open transaction");
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(1));
            producer.sendOffsetsToTransaction(offsets, first.groupMetadata());
            producer.commitTransaction();
            check(drain(first, Duration.ofSeconds(15)).equals(Set.of("open")), "committed record missing");
        }
        try (Admin client = Admin.create(admin)) {
            TransactionDescription described =
                    client.describeTransactions(List.of(transactionalId)).all().get().get(transactionalId);
            check(described.state().toString().equals("CompleteCommit"), "describe " + described.state());
            boolean listed = client.listTransactions().all().get().stream()
                    .anyMatch(row -> row.transactionalId().equals(transactionalId));
            check(listed, "transaction missing from the list");
        }

        try (KafkaConsumer<String, String> resumed = consumer(bootstrap, group)) {
            resumed.subscribe(List.of(topic));
            check(drain(resumed, Duration.ofSeconds(3)).isEmpty(), "offset commit did not hide the committed record");

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k", "aborted")).get();
            producer.abortTransaction();
            check(drain(resumed, Duration.ofSeconds(2)).isEmpty(), "read_committed saw an abort");

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k", "after")).get();
            producer.commitTransaction();
            Set<String> seen = drain(resumed, Duration.ofSeconds(15));
            check(seen.equals(Set.of("after")), "after abort: " + seen);
        }
        producer.close();
        System.out.println("java transaction ok: open hidden, commit and offset together, abort hidden");
    }

    static KafkaConsumer<String, String> consumer(String bootstrap, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    static void awaitAssignment(KafkaConsumer<String, String> consumer) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (consumer.assignment().isEmpty()) {
            check(System.currentTimeMillis() < deadline, "assignment did not settle");
            consumer.poll(Duration.ofMillis(200));
        }
    }

    static Set<String> drain(KafkaConsumer<String, String> consumer, Duration budget) {
        Set<String> seen = new HashSet<>();
        long deadline = System.currentTimeMillis() + budget.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                seen.add(record.value());
            }
            if (!seen.isEmpty()) {
                return seen;
            }
        }
        return seen;
    }

    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
}
