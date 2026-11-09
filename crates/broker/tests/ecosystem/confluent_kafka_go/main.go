// confluent-kafka-go round trip for the ecosystem harness: create a topic,
// produce, and consume it back in a group. Argument: bootstrap address.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) != 2 {
		fatal(fmt.Errorf("usage: confluent-kafka-go bootstrap"))
	}
	bootstrap := os.Args[1]
	topic := fmt.Sprintf("ckgo-%d", time.Now().UnixNano())
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}})
	if err != nil {
		fatal(err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			fatal(result.Error)
		}
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"acks":              "all",
	})
	if err != nil {
		fatal(err)
	}
	defer producer.Close()
	delivery := make(chan kafka.Event, 1)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("k"),
		Value:          []byte("from-confluent-kafka-go"),
	}, delivery); err != nil {
		fatal(err)
	}
	event := <-delivery
	report, ok := event.(*kafka.Message)
	if !ok || report.TopicPartition.Error != nil {
		if ok {
			fatal(report.TopicPartition.Error)
		}
		fatal(fmt.Errorf("produce: %v", event))
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"group.id":          topic + "-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Subscribe(topic, nil); err != nil {
		fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	msg, err := consumer.ReadMessage(time.Until(deadline))
	if err != nil {
		fatal(err)
	}
	if string(msg.Value) != "from-confluent-kafka-go" {
		fatal(fmt.Errorf("read %s", msg.Value))
	}
	fmt.Println("confluent-kafka-go ok")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
