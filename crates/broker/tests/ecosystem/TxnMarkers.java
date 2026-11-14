// What a client sees of a transactional partition when the log has no
// control batches, for the ecosystem harness (ADR 0021).
// Arguments: bootstrap servers, topic (one partition), consumer group.
//
// The log is: commit c1 c2 c3, abort a1 a2, commit c4, abort a3, then
// commit o1 and abort a4. Kafka
// would also hold one marker after each transaction. The checks are the
// properties tools rely on: a read_committed consumer reaches the end
// offset, so its lag is zero; read_uncommitted sees aborted records; and the
// end offset of a read_committed consumer stops at an open transaction.
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public class TxnMarkers {
    static int failures = 0;

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        TopicPartition tp = new TopicPartition(topic, 0);

        try (KafkaProducer<String, String> producer = producer(bootstrap)) {
            producer.initTransactions();
            transaction(producer, topic, List.of("c1", "c2", "c3"), true);
            transaction(producer, topic, List.of("a1", "a2"), false);
            transaction(producer, topic, List.of("c4"), true);
            transaction(producer, topic, List.of("a3"), false);

            try (KafkaConsumer<String, String> committed = consumer(bootstrap, group, "read_committed")) {
                committed.assign(List.of(tp));
                committed.seekToBeginning(List.of(tp));
                List<String> seen = read(committed, 4);
                long end = committed.endOffsets(List.of(tp)).get(tp);
                // The trailing abort is past every record this consumer
                // returns; Kafka moves the position over it and its marker.
                long position = settle(committed, tp, end);
                committed.commitSync();
                System.out.println("COMMITTED " + seen);
                System.out.println("POSITION " + position + " END " + end);
                check(seen.equals(List.of("0:c1", "1:c2", "2:c3", "5:c4")), "read_committed records " + seen);
                check(position == end, "read_committed position " + position + " reaches end " + end);
            }

            try (KafkaConsumer<String, String> uncommitted = consumer(bootstrap, group + "-ru", "read_uncommitted")) {
                uncommitted.assign(List.of(tp));
                uncommitted.seekToBeginning(List.of(tp));
                List<String> seen = read(uncommitted, 7);
                System.out.println("UNCOMMITTED " + seen);
                check(seen.size() == 7, "read_uncommitted sees aborted records " + seen);
            }

            producer.beginTransaction();
            long open = producer.send(new ProducerRecord<>(topic, "k", "o1")).get().offset();
            try (KafkaConsumer<String, String> latest = consumer(bootstrap, group + "-latest", "read_committed")) {
                long stable = latest.endOffsets(List.of(tp)).get(tp);
                System.out.println("OPEN " + open + " STABLE_END " + stable);
                check(stable == open, "read_committed end offset " + stable + " stops at the open batch " + open);
                latest.assign(List.of(tp));
                latest.seekToEnd(List.of(tp));
                latest.position(tp);
                producer.commitTransaction();
                List<String> seen = read(latest, 1);
                System.out.println("AFTER_COMMIT " + seen);
                check(seen.equals(List.of(open + ":o1")), "a consumer at the end sees the transaction commit " + seen);
            }
            // End on an abort again, for the librdkafka reader that follows.
            transaction(producer, topic, List.of("a4"), false);
        }
        if (failures > 0) {
            System.out.println("txn markers: " + failures + " check(s) failed");
            System.exit(1);
        }
        System.out.println("txn markers ok");
    }

    static void transaction(KafkaProducer<String, String> producer, String topic, List<String> values, boolean commit)
            throws Exception {
        producer.beginTransaction();
        for (String value : values) {
            producer.send(new ProducerRecord<>(topic, "k", value)).get();
        }
        if (commit) {
            producer.commitTransaction();
        } else {
            producer.abortTransaction();
        }
    }

    static List<String> read(KafkaConsumer<String, String> consumer, int want) {
        List<String> seen = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 20_000;
        while (seen.size() < want && System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                seen.add(record.offset() + ":" + record.value());
            }
        }
        return seen;
    }

    static long settle(KafkaConsumer<String, String> consumer, TopicPartition tp, long end) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (consumer.position(tp) < end && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(500));
        }
        return consumer.position(tp);
    }

    static void check(boolean ok, String what) {
        System.out.println((ok ? "ok   " : "FAIL ") + what);
        if (!ok) {
            failures++;
        }
    }

    static KafkaProducer<String, String> producer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "markers-" + UUID.randomUUID());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    static KafkaConsumer<String, String> consumer(String bootstrap, String group, String isolation) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }
}
