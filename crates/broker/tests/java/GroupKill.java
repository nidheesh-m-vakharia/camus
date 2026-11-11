// Two members share a group and do not commit. One process is SIGKILLed
// after it has read a batch. The survivor must be handed those records again.
// args: <bootstrap>            runs classic, then group.protocol=consumer
//       member <bootstrap> <topic> <group> <role> <protocol> <seen> <marker> <need>
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class GroupKill {
    static final int COUNT = 40;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("member")) {
            member(args);
            return;
        }
        String bootstrap = args[0];
        for (String protocol : List.of("classic", "consumer")) {
            run(bootstrap, protocol);
        }
        System.out.println("java kill ok: uncommitted records redelivered, classic and consumer");
    }

    static void run(String bootstrap, String protocol) throws Exception {
        String topic = "k" + UUID.randomUUID().toString().substring(0, 8);
        String group = "kill-" + UUID.randomUUID().toString().substring(0, 8);
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(topic, 4, (short) 3))).all().get();
        }
        produce(bootstrap, topic);
        Path dir = Files.createTempDirectory("group-kill");
        Path victimSeen = dir.resolve("victim");
        Path survivorSeen = dir.resolve("survivor");
        Path marker = dir.resolve("killed");
        Path need = dir.resolve("need");
        Files.writeString(victimSeen, "");
        Files.writeString(survivorSeen, "");
        Process victim = spawn(bootstrap, topic, group, "victim", protocol, victimSeen, marker, need);
        Process survivor =
                spawn(bootstrap, topic, group, "survivor", protocol, survivorSeen, marker, need);
        long deadline = System.currentTimeMillis() + 45_000;
        Set<String> snapshot = null;
        while (System.currentTimeMillis() < deadline) {
            Set<String> victimValues = read(victimSeen);
            Set<String> survivorValues = read(survivorSeen);
            if (!victimValues.isEmpty() && !survivorValues.isEmpty() && !survivorValues.containsAll(victimValues)) {
                snapshot = victimValues;
                break;
            }
            Thread.sleep(200);
        }
        check(snapshot != null, protocol + ": members never split the log");
        victim.destroyForcibly();
        victim.waitFor(10, TimeUnit.SECONDS);
        Files.writeString(need, String.join("\n", snapshot) + "\n");
        Files.writeString(marker, "killed\n");
        check(survivor.waitFor(45, TimeUnit.SECONDS), protocol + ": survivor did not finish");
        check(survivor.exitValue() == 0, protocol + ": survivor failed");
    }

    static Process spawn(
            String bootstrap,
            String topic,
            String group,
            String role,
            String protocol,
            Path seen,
            Path marker,
            Path need)
            throws IOException {
        String cp = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(
                "java",
                "-cp",
                cp,
                "GroupKill",
                "member",
                bootstrap,
                topic,
                group,
                role,
                protocol,
                seen.toString(),
                marker.toString(),
                need.toString());
        builder.inheritIO();
        return builder.start();
    }

    static void member(String[] args) {
        String bootstrap = args[1];
        String topic = args[2];
        String group = args[3];
        String role = args[4];
        String protocol = args[5];
        Path seen = Path.of(args[6]);
        Path marker = Path.of(args[7]);
        Path need = Path.of(args[8]);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, role);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        // The consumer protocol takes its session from the broker. Classic
        // members still send these, and they have to clear the broker minimum.
        if (protocol.equals("classic")) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        }
        Set<String> got = new HashSet<>();
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 90_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (got.add(record.value())) {
                        Files.writeString(
                                seen,
                                Files.readString(seen) + record.value() + "\n");
                    }
                }
                if (role.equals("survivor") && Files.exists(marker)) {
                    Set<String> required = read(need);
                    if (got.containsAll(required) && got.size() >= COUNT) {
                        return;
                    }
                }
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        throw new AssertionError(role + " " + protocol + " timed out with " + got.size());
    }

    static void produce(String bootstrap, String topic) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (int index = 0; index < COUNT; index++) {
                producer.send(new ProducerRecord<>(topic, index % 4, String.valueOf(index), "v" + index))
                        .get();
            }
        }
    }

    static Set<String> read(Path path) throws IOException {
        Set<String> out = new HashSet<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readString(path).split("\n")) {
            if (line.startsWith("v")) {
                out.add(line.trim());
            }
        }
        return out;
    }

    static void check(boolean ok, String message) {
        if (!ok) {
            throw new AssertionError(message);
        }
    }
}
