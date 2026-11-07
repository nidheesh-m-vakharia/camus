// kafka-clients 4.4.0 with group.protocol=consumer (KIP-848): two members
// share a group, commit, and leave; a third member resumes from the offsets.
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class GroupConsumerRoundTrip {
    static String bootstrap;
    static String topic = "c" + UUID.randomUUID().toString().substring(0, 8);
    static String group = "consumer-" + UUID.randomUUID().toString().substring(0, 8);

    public static void main(String[] args) throws Exception {
        bootstrap = args[0];
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(topic, 4, (short) 3))).all().get();
        }
        produce(0, 40);
        KafkaConsumer<String, String> first = consumer("first");
        KafkaConsumer<String, String> second = consumer("second");
        Set<String> seen = drain(List.of(first, second), 40);
        check(seen.equals(range(0, 40)), "first 40: " + new TreeSet<>(seen));
        settle(first, second);
        check(!first.assignment().isEmpty() && !second.assignment().isEmpty(), "both members hold partitions");
        check(first.assignment().size() + second.assignment().size() == 4, "four partitions assigned");
        for (KafkaConsumer<String, String> member : List.of(first, second)) {
            member.commitSync();
            member.close();
        }
        produce(40, 20);
        KafkaConsumer<String, String> third = consumer("third");
        Set<String> resumed = drain(List.of(third), 20);
        check(resumed.equals(range(40, 60)), "resumed: " + new TreeSet<>(resumed));
        ConsumerRecords<String, String> extra = third.poll(Duration.ofSeconds(2));
        check(extra.isEmpty(), "re-read " + extra.count() + " records");
        third.commitSync();
        third.close();
        System.out.println("java consumer group ok: 40 shared by two members, 20 resumed by a third");
    }

    static void settle(KafkaConsumer<String, String> first, KafkaConsumer<String, String> second) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (first.assignment().isEmpty() || second.assignment().isEmpty()) {
            check(System.currentTimeMillis() < deadline, "assignment did not settle");
            first.poll(Duration.ofMillis(200));
            second.poll(Duration.ofMillis(200));
        }
    }

    static void produce(int first, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (int index = first; index < first + count; index++) {
                producer.send(new ProducerRecord<>(topic, String.valueOf(index), "v" + index)).get();
            }
        }
    }

    static KafkaConsumer<String, String> consumer(String name) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer =
            new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    static Set<String> drain(List<KafkaConsumer<String, String>> members, int want) {
        Set<String> seen = new HashSet<>();
        long deadline = System.currentTimeMillis() + 60_000;
        while (seen.size() < want) {
            check(System.currentTimeMillis() < deadline, "only " + seen.size() + " of " + want);
            for (KafkaConsumer<String, String> member : members) {
                for (ConsumerRecord<String, String> record : member.poll(Duration.ofMillis(200))) {
                    seen.add(record.value());
                }
            }
        }
        return seen;
    }

    static Set<String> range(int from, int to) {
        Set<String> out = new HashSet<>();
        for (int index = from; index < to; index++) out.add("v" + index);
        return out;
    }

    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
}
