// confluent-kafka-dotnet round trip for the ecosystem harness: create a
// topic, produce, and consume it back in a group. Argument: bootstrap address.
using Confluent.Kafka;
using Confluent.Kafka.Admin;

if (args.Length != 1)
{
    Console.Error.WriteLine("usage: confluent-kafka-dotnet bootstrap");
    Environment.Exit(2);
}

var bootstrap = args[0];
var topic = "ckdn-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    await admin.CreateTopicsAsync(new[]
    {
        new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 },
    });
}

using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All,
}).Build())
{
    var result = await producer.ProduceAsync(topic, new Message<string, string>
    {
        Key = "k",
        Value = "from-confluent-kafka-dotnet",
    });
    if (result.Status == PersistenceStatus.NotPersisted)
    {
        throw new Exception("produce was not persisted");
    }
}

using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = topic + "-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
}).Build())
{
    consumer.Subscribe(topic);
    var record = consumer.Consume(TimeSpan.FromSeconds(30));
    if (record?.Message?.Value != "from-confluent-kafka-dotnet")
    {
        throw new Exception($"read {record?.Message?.Value}");
    }
    consumer.Close();
}

Console.WriteLine("confluent-kafka-dotnet ok");
