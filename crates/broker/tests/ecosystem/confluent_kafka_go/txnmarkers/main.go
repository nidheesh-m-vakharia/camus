// librdkafka reading a transactional partition that ends in an aborted
// transaction, for the ecosystem harness (ADR 0021). The partition is the one
// TxnMarkers.java wrote. Arguments: bootstrap address, topic, the values a
// read_committed consumer must see in order.
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) != 4 {
		fatal(fmt.Errorf("usage: txnmarkers bootstrap topic v1,v2,..."))
	}
	bootstrap, topic := os.Args[1], os.Args[2]
	want := strings.Split(os.Args[3], ",")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           fmt.Sprintf("txnmarkers-%d", time.Now().UnixNano()),
		"isolation.level":    "read_committed",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		fatal(err)
	}
	defer consumer.Close()
	partition := kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.OffsetBeginning}
	if err := consumer.Assign([]kafka.TopicPartition{partition}); err != nil {
		fatal(err)
	}
	var seen []string
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < len(want) && time.Now().Before(deadline) {
		message, err := consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
				continue
			}
			fatal(err)
		}
		seen = append(seen, string(message.Value))
	}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		fatal(fmt.Errorf("read_committed saw %v, want %v", seen, want))
	}
	_, high, err := consumer.QueryWatermarkOffsets(topic, 0, 10000)
	if err != nil {
		fatal(err)
	}
	// The partition ends in an aborted transaction. librdkafka moves its
	// position past it, as past Kafka's abort marker.
	var position int64
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := consumer.ReadMessage(200 * time.Millisecond); err == nil {
			fatal(fmt.Errorf("read_committed saw a record past the committed ones"))
		}
		positions, err := consumer.Position([]kafka.TopicPartition{{Topic: &topic, Partition: 0}})
		if err != nil {
			fatal(err)
		}
		position = int64(positions[0].Offset)
		if position == high {
			break
		}
	}
	fmt.Printf("librdkafka read_committed %v position %d end %d\n", seen, position, high)
	if position != high {
		fatal(fmt.Errorf("position %d never reached the end %d", position, high))
	}
	fmt.Println("librdkafka txn markers ok")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "librdkafka txn markers:", err)
	os.Exit(1)
}
