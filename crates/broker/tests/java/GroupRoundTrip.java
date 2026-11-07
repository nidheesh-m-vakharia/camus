// kafka-clients 4.4.0 (classic group protocol): two consumers share a group,
// commit, and leave; a third member resumes from the committed offsets.
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class GroupRoundTrip {
    static String bootstrap;
    static String topic = "j" + UUID.randomUUID().toString().substring(0, 8);
    static String group = "group-" + UUID.randomUUID().toString().substring(0, 8);

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
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin client = Admin.create(adminProps)) {
            boolean listed = client.listGroups().all().get().stream()
                    .anyMatch(row -> row.groupId().equals(group));
            check(listed, "group missing from list");
            String described = client.describeConsumerGroups(List.of(group))
                    .describedGroups().get(group).get().groupId();
            check(described.equals(group), "describe " + described);
            client.deleteConsumerGroups(List.of(group)).all().get();
        }
        System.out.println("java group ok: 40 shared by two members, 20 resumed by a third");
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
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        KafkaConsumer<String, String> consumer =
            new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    // A rebalance before the first commit may deliver a record twice.
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
