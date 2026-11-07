// franz-go: a transaction, a compacted log, or an ACL denial.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	mode, bootstrap := os.Args[1], os.Args[2]
	switch mode {
	case "txn":
		transaction(bootstrap)
	case "compact":
		compact(bootstrap)
	case "acl":
		acl(bootstrap)
	case "admin":
		adminRound(bootstrap)
	default:
		panic(mode)
	}
}

func transaction(bootstrap string) {
	ctx := context.Background()
	topic := fmt.Sprintf("t%d", time.Now().UnixNano()%1_000_000_000)
	group := fmt.Sprintf("g%d", time.Now().UnixNano()%1_000_000_000)
	txn := fmt.Sprintf("txn-%d", time.Now().UnixNano()%1_000_000_000)
	admin, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	must(err)
	created, err := kadm.NewClient(admin).CreateTopic(ctx, 1, 1, nil, topic)
	must(err)
	must(created.Err)
	admin.Close()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.TransactionalID(txn),
		kgo.TransactionTimeout(10*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	must(err)
	defer producer.Close()
	consumer := reader(bootstrap, group, topic)
	defer consumer.Close()
	await(ctx, consumer)

	must(producer.BeginTransaction())
	must(producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("open")}).FirstErr())
	check(len(poll(ctx, consumer, 2*time.Second)) == 0, "saw an open transaction")
	must(producer.EndTransaction(ctx, kgo.TryCommit))
	check(values(poll(ctx, consumer, 15*time.Second))["open"], "committed record missing")

	must(producer.BeginTransaction())
	must(producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("aborted")}).FirstErr())
	must(producer.EndTransaction(ctx, kgo.TryAbort))
	check(len(poll(ctx, consumer, 2*time.Second)) == 0, "saw an abort")

	must(producer.BeginTransaction())
	must(producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("after")}).FirstErr())
	must(producer.EndTransaction(ctx, kgo.TryCommit))
	seen := values(poll(ctx, consumer, 15*time.Second))
	check(seen["after"] && !seen["aborted"], fmt.Sprintf("after abort %v", seen))
	fmt.Println("franz-go transaction ok")
}

func compact(bootstrap string) {
	ctx := context.Background()
	topic := fmt.Sprintf("c%d", time.Now().UnixNano()%1_000_000_000)
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	must(err)
	defer client.Close()
	adm := kadm.NewClient(client)
	created, err := adm.CreateTopic(ctx, 1, 1, map[string]*string{"durability.mode": kadm.StringPtr("r2")}, topic)
	must(err)
	must(created.Err)
	value := "compact"
	altered, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
		Op: kadm.SetConfig, Name: "cleanup.policy", Value: &value,
	}}, topic)
	must(err)
	must(altered[0].Err)
	plain, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite())
	must(err)
	for _, record := range [][2]string{{"a", "1"}, {"b", "1"}, {"a", "2"}} {
		must(plain.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(record[0]), Value: []byte(record[1])}).FirstErr())
	}
	plain.Close()
	deadline := time.Now().Add(20 * time.Second)
	var seen map[string]string
	for time.Now().Before(deadline) {
		reader, err := kgo.NewClient(
			kgo.SeedBrokers(bootstrap),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		)
		must(err)
		seen = map[string]string{}
		slice, cancel := context.WithTimeout(ctx, time.Second)
		fetches := reader.PollFetches(slice)
		cancel()
		reader.Close()
		fetches.EachRecord(func(record *kgo.Record) {
			seen[string(record.Key)] = string(record.Value)
		})
		if seen["a"] == "2" && seen["b"] == "1" && len(seen) == 2 {
			fmt.Println("franz-go compaction ok")
			return
		}
	}
	panic(fmt.Sprintf("compacted %v", seen))
}

func acl(bootstrap string) {
	ctx := context.Background()
	topic := fmt.Sprintf("a%d", time.Now().UnixNano()%1_000_000_000)
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	must(err)
	defer client.Close()
	adm := kadm.NewClient(client)
	created, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	must(err)
	must(created.Err)
	produce(ctx, bootstrap, topic, "k", "1")
	_, err = adm.CreateACLs(ctx, kadm.NewACLs().Topics(topic).Allow("User:ANONYMOUS").Operations(kadm.OpWrite))
	must(err)
	produce(ctx, bootstrap, topic, "k", "2")
	check(readErr(ctx, bootstrap, topic), "fetch was allowed with only WRITE")
	_, err = adm.CreateACLs(ctx, kadm.NewACLs().Topics(topic).Allow("User:ANONYMOUS").Operations(kadm.OpRead))
	must(err)
	_, err = adm.CreateACLs(ctx, kadm.NewACLs().Topics(topic).Deny("User:ANONYMOUS").Operations(kadm.OpWrite))
	must(err)
	check(!readErr(ctx, bootstrap, topic), "READ allow did not fetch")
	err = produceErr(ctx, bootstrap, topic, "k", "3")
	check(err != nil && err.Error() != "", "DENY WRITE still produced")
	fmt.Println("franz-go acl ok")
}

func adminRound(bootstrap string) {
	ctx := context.Background()
	topic := fmt.Sprintf("e%d", time.Now().UnixNano()%1_000_000_000)
	group := fmt.Sprintf("g%d", time.Now().UnixNano()%1_000_000_000)
	txn := fmt.Sprintf("txn-%d", time.Now().UnixNano()%1_000_000_000)
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	must(err)
	defer client.Close()
	adm := kadm.NewClient(client)
	created, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	must(err)
	must(created.Err)
	retention := "1000"
	altered, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
		Op: kadm.SetConfig, Name: "retention.ms", Value: &retention,
	}}, topic)
	must(err)
	must(altered[0].Err)
	state := "2000"
	replaced, err := adm.AlterTopicConfigsState(ctx, []kadm.AlterConfig{{
		Name: "retention.ms", Value: &state,
	}}, topic)
	must(err)
	must(replaced[0].Err)
	configs, err := adm.DescribeTopicConfigs(ctx, topic)
	must(err)
	found := false
	for _, resource := range configs {
		for _, entry := range resource.Configs {
			if entry.Key == "retention.ms" && entry.Value != nil && *entry.Value == "2000" {
				found = true
			}
		}
	}
	check(found, "retention was not 2000")
	grown, err := adm.UpdatePartitions(ctx, 2, topic)
	must(err)
	must(grown[topic].Err)
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 0})
	deleted, err := adm.DeleteRecords(ctx, offsets)
	must(err)
	must(deleted.Error())
	member, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	must(err)
	await(ctx, member)
	done := make(chan error, 1)
	member.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Offset: 0}},
	}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		done <- err
	})
	must(<-done)
	member.Close()
	removed, err := adm.DeleteOffsets(ctx, group, kadm.TopicsSet{topic: {0: struct{}{}}})
	must(err)
	must(removed.Error())
	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.TransactionalID(txn), kgo.TransactionTimeout(10*time.Second))
	must(err)
	must(producer.BeginTransaction())
	must(producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr())
	listed, err := adm.ListTransactions(ctx, nil, nil)
	must(err)
	_, ok := listed[txn]
	check(ok, "list transactions missed the id")
	txns, err := adm.DescribeTransactions(ctx, txn)
	must(err)
	check(txns[txn].Err == nil, "describe transaction")
	must(producer.EndTransaction(ctx, kgo.TryAbort))
	producer.Close()
	gone, err := adm.DeleteTopics(ctx, topic)
	must(err)
	must(gone[topic].Err)
	_, err = adm.CreateACLs(ctx, kadm.NewACLs().Topics(topic).Allow("User:ANONYMOUS").Operations(kadm.OpRead))
	must(err)
	described, err := adm.DescribeACLs(ctx, kadm.NewACLs().Topics(topic).Allow("User:ANONYMOUS").AllowHosts("*").Operations(kadm.OpRead))
	must(err)
	check(len(described) > 0 && len(described[0].Described) > 0, fmt.Sprintf("describe acls: %+v", described))
	_, err = adm.DeleteACLs(ctx, kadm.NewACLs().Topics(topic).Allow("User:ANONYMOUS").AllowHosts("*").Operations(kadm.OpRead))
	must(err)
	fmt.Println("franz-go admin ok")
}

func produce(ctx context.Context, bootstrap, topic, key, value string) {
	must(produceErr(ctx, bootstrap, topic, key, value))
}

func produceErr(ctx context.Context, bootstrap, topic, key, value string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite())
	if err != nil {
		return err
	}
	defer client.Close()
	return client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value)}).FirstErr()
}

func readErr(ctx context.Context, bootstrap, topic string) bool {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
	)
	must(err)
	defer client.Close()
	slice, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	fetches := client.PollFetches(slice)
	return len(fetches.Errors()) > 0 && len(fetches.Records()) == 0
}

func reader(bootstrap, group, topic string) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	must(err)
	return client
}

func await(ctx context.Context, client *kgo.Client) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		slice, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		client.PollFetches(slice)
		cancel()
		if member, _ := client.GroupMetadata(); member != "" {
			return
		}
	}
	panic("assignment did not settle")
}

func poll(ctx context.Context, client *kgo.Client, budget time.Duration) []*kgo.Record {
	deadline := time.Now().Add(budget)
	var records []*kgo.Record
	for time.Now().Before(deadline) {
		slice, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := client.PollFetches(slice)
		cancel()
		records = append(records, fetches.Records()...)
		if len(records) > 0 && budget > 3*time.Second {
			return records
		}
	}
	return records
}

func values(records []*kgo.Record) map[string]bool {
	out := map[string]bool{}
	for _, record := range records {
		out[string(record.Value)] = true
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
