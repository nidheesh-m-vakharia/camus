// kafka-clients 4.4.0: a group.protocol=consumer member joins, then
// Admin.describeConsumerGroups reads it back through ConsumerGroupDescribe.
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class DescribeConsumerGroup {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = "d" + UUID.randomUUID().toString().substring(0, 8);
        String group = "dg-" + UUID.randomUUID().toString().substring(0, 8);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) -1))).all().get();
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                    props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(List.of(topic));
                long deadline = System.currentTimeMillis() + 30_000;
                while (consumer.assignment().isEmpty()) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("the member was not assigned");
                    }
                    consumer.poll(Duration.ofMillis(200));
                }
                var described = admin.describeConsumerGroups(List.of(group))
                    .describedGroups()
                    .get(group)
                    .get(30, TimeUnit.SECONDS);
                if (described.type() != GroupType.CONSUMER) {
                    throw new AssertionError("type " + described.type());
                }
                if (described.members().isEmpty()) {
                    throw new AssertionError("no members: " + described);
                }
                System.out.println("consumer group describe ok");
            }
        }
    }
}
