// franz-go: two consumers share a group, commit, and leave; a third member
// resumes from the committed offsets.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers []string
	topic   = fmt.Sprintf("f%d", time.Now().UnixNano()%1_000_000_000)
	group   = fmt.Sprintf("group-%d", time.Now().UnixNano()%1_000_000_000)
)

func main() {
	brokers = strings.Split(os.Args[1], ",")
	ctx := context.Background()
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	must(err)
	created, err := kadm.NewClient(admin).CreateTopic(ctx, 4, 3, nil, topic)
	must(err)
	must(created.Err)
	admin.Close()

	produce(ctx, 0, 40)
	first, second := consumer("first"), consumer("second")
	seen := drain(ctx, []*kgo.Client{first, second}, 40)
	check(equal(seen, span(0, 40)), "first 40")
	for _, member := range []*kgo.Client{first, second} {
		must(member.CommitUncommittedOffsets(ctx))
		member.Close()
	}
	produce(ctx, 40, 20)
	third := consumer("third")
	resumed := drain(ctx, []*kgo.Client{third}, 20)
	check(equal(resumed, span(40, 60)), fmt.Sprintf("resumed %v", keys(resumed)))
	quiet, cancel := context.WithTimeout(ctx, 2*time.Second)
	extra := third.PollFetches(quiet)
	cancel()
	check(len(extra.Records()) == 0, fmt.Sprintf("re-read %d records", len(extra.Records())))
	must(third.CommitUncommittedOffsets(ctx))
	third.Close()
	fmt.Println("franz-go group ok: 40 shared by two members, 20 resumed by a third")
}

func produce(ctx context.Context, first, count int) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite())
	must(err)
	defer client.Close()
	for index := first; index < first+count; index++ {
		record := &kgo.Record{Topic: topic, Key: []byte(strconv.Itoa(index)), Value: []byte("v" + strconv.Itoa(index))}
		must(client.ProduceSync(ctx, record).FirstErr())
	}
}

func consumer(name string) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(name),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(time.Second),
	)
	must(err)
	return client
}

// A rebalance before the first commit may deliver a record twice.
func drain(ctx context.Context, members []*kgo.Client, want int) map[string]bool {
	seen := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < want {
		check(time.Now().Before(deadline), fmt.Sprintf("only %d of %d", len(seen), want))
		for _, member := range members {
			short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			fetches := member.PollFetches(short)
			cancel()
			for _, record := range fetches.Records() {
				seen[string(record.Value)] = true
			}
		}
	}
	return seen
}

func span(from, to int) map[string]bool {
	out := map[string]bool{}
	for index := from; index < to; index++ {
		out["v"+strconv.Itoa(index)] = true
	}
	return out
}

func equal(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if !b[key] {
			return false
		}
	}
	return true
}

func keys(m map[string]bool) []string {
	out := []string{}
	for key := range m {
		out = append(out, key)
	}
	return out
}

func check(ok bool, message string) {
	if !ok {
		fatal(fmt.Errorf("%s", message))
	}
}

func must(err error) {
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
