import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** One SASL_SSL connection: create, produce, and assign-fetch each codec. */
public class SaslRoundTrip {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String trust = args[1];
        Properties common = new Properties();
        common.put("bootstrap.servers", bootstrap);
        common.put("security.protocol", "SASL_SSL");
        common.put("sasl.mechanism", "PLAIN");
        common.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"camus\" password=\"camus-secret\";");
        common.put("ssl.truststore.type", "PEM");
        common.put("ssl.truststore.location", trust);
        common.put("request.timeout.ms", "10000");
        common.put("default.api.timeout.ms", "10000");

        String[] codecs = {"none", "gzip", "snappy", "lz4", "zstd"};
        for (String codec : codecs) {
            String topic = "j" + codec + UUID.randomUUID().toString().substring(0, 6);
            byte[] value = codec.getBytes(StandardCharsets.UTF_8);
            try (Admin admin = Admin.create(common)) {
                admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get();
            }
            Properties producer = new Properties();
            producer.putAll(common);
            producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producer.put(ProducerConfig.ACKS_CONFIG, "all");
            producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            producer.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
            producer.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
            try (KafkaProducer<String, byte[]> kafka = new KafkaProducer<>(producer)) {
                kafka.send(new ProducerRecord<>(topic, "k", value)).get();
            }
            Properties consumer = new Properties();
            consumer.putAll(common);
            consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            TopicPartition partition = new TopicPartition(topic, 0);
            try (KafkaConsumer<String, byte[]> kafka = new KafkaConsumer<>(consumer)) {
                kafka.assign(Collections.singleton(partition));
                kafka.seekToBeginning(Collections.singleton(partition));
                long deadline = System.currentTimeMillis() + 15000;
                boolean found = false;
                while (System.currentTimeMillis() < deadline) {
                    for (var record : kafka.poll(Duration.ofMillis(400))) {
                        if (java.util.Arrays.equals(record.value(), value)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalStateException("missing " + codec);
                }
            }
            System.out.println("ok " + codec);
        }
    }
}
