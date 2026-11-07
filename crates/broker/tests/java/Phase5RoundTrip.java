// kafka-clients 4.4.0: compaction and ACL denial.
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.*;

public class Phase5RoundTrip {
    public static void main(String[] args) throws Exception {
        if (args[0].equals("compact")) compact(args[1]);
        else if (args[0].equals("acl")) acl(args[1]);
        else throw new AssertionError(args[0]);
    }

    static void compact(String bootstrap) throws Exception {
        String topic = "c" + UUID.randomUUID().toString().substring(0, 8);
        Properties adminProps = admin(bootstrap);
        try (Admin client = Admin.create(adminProps)) {
            NewTopic created = new NewTopic(topic, 1, (short) 1);
            created.configs(Map.of("durability.mode", "r2"));
            client.createTopics(List.of(created)).all().get();
            client.incrementalAlterConfigs(Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, topic),
                    List.of(new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOp.OpType.SET))))
                    .all().get();
        }
        produce(bootstrap, topic, "a", "1");
        produce(bootstrap, topic, "b", "1");
        produce(bootstrap, topic, "a", "2");
        Map<String, String> seen = waitCompact(bootstrap, topic);
        check(seen.equals(Map.of("b", "1", "a", "2")), "compacted " + seen);
        System.out.println("java compaction ok");
    }

    static void acl(String bootstrap) throws Exception {
        String topic = "a" + UUID.randomUUID().toString().substring(0, 8);
        try (Admin client = Admin.create(admin(bootstrap))) {
            client.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        produce(bootstrap, topic, "k", "1");
        try (Admin client = Admin.create(admin(bootstrap))) {
            client.createAcls(List.of(binding(topic, AclOperation.WRITE, AclPermissionType.ALLOW))).all().get();
        }
        produce(bootstrap, topic, "k", "2");
        check(failsRead(bootstrap, topic), "fetch was allowed with only WRITE");
        try (Admin client = Admin.create(admin(bootstrap))) {
            client.createAcls(List.of(
                    binding(topic, AclOperation.READ, AclPermissionType.ALLOW),
                    binding(topic, AclOperation.WRITE, AclPermissionType.DENY)))
                    .all().get();
            var described = client.describeAcls(AclBindingFilter.ANY).values().get();
            check(described.size() >= 3, "describe " + described.size());
        }
        check(!failsRead(bootstrap, topic), "READ allow did not fetch");
        boolean denied = false;
        try {
            produce(bootstrap, topic, "k", "3");
        } catch (ExecutionException err) {
            denied = err.getCause() instanceof TopicAuthorizationException
                    || String.valueOf(err.getCause()).contains("AUTHORIZATION");
        }
        check(denied, "DENY WRITE still produced");
        System.out.println("java acl ok");
    }

    static AclBinding binding(String topic, AclOperation operation, AclPermissionType permission) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:ANONYMOUS", "*", operation, permission));
    }

    static Properties admin(String bootstrap) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        return props;
    }

    static void produce(String bootstrap, String topic, String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    static boolean failsRead(String bootstrap, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "none");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            TopicPartition part = new TopicPartition(topic, 0);
            consumer.assign(List.of(part));
            consumer.seek(part, 0);
            consumer.poll(Duration.ofSeconds(2));
            return false;
        } catch (TopicAuthorizationException err) {
            return true;
        }
    }

    static Map<String, String> waitCompact(String bootstrap, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            TopicPartition part = new TopicPartition(topic, 0);
            long deadline = System.currentTimeMillis() + 20_000;
            Map<String, String> seen = Map.of();
            while (System.currentTimeMillis() < deadline) {
                consumer.assign(List.of(part));
                consumer.seek(part, 0);
                seen = new HashMap<>();
                long slice = System.currentTimeMillis() + 1000;
                while (System.currentTimeMillis() < slice) {
                    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                        seen.put(record.key(), record.value());
                    }
                }
                if (seen.equals(Map.of("b", "1", "a", "2"))) return seen;
            }
            return seen;
        }
    }

    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
}
