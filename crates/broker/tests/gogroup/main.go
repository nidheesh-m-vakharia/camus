// franz-go: two consumers share a group, commit, and leave; a third member
// resumes from the committed offsets.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	if len(os.Args) > 1 && os.Args[1] == "member" {
		goMember()
		return
	}
	brokers = strings.Split(os.Args[1], ",")
	if len(os.Args) > 2 && os.Args[2] == "kill" {
		for _, protocol := range []string{"classic", "consumer"} {
			kill(protocol)
		}
		fmt.Println("franz-go kill ok: uncommitted records redelivered, classic and consumer")
		return
	}
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
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	must(err)
	adm := kadm.NewClient(adminClient)
	listed, err := adm.ListGroups(ctx)
	must(err)
	_, ok := listed[group]
	check(ok, "group missing from list")
	described, err := adm.DescribeGroups(ctx, group)
	must(err)
	check(described[group].Group == group, "describe")
	_, err = adm.DeleteGroup(ctx, group)
	must(err)
	adminClient.Close()
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

// kill starts two members that do not commit, SIGKILLs one after it has
// records the other has not seen, and waits for the survivor to read them.
func kill(protocol string) {
	ctx := context.Background()
	topic = fmt.Sprintf("k%d", time.Now().UnixNano()%1_000_000_000)
	group = fmt.Sprintf("kill-%d", time.Now().UnixNano()%1_000_000_000)
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	must(err)
	created, err := kadm.NewClient(admin).CreateTopic(ctx, 4, 3, nil, topic)
	must(err)
	must(created.Err)
	admin.Close()
	producePinned(ctx, topic)
	dir, err := os.MkdirTemp("", "group-kill")
	must(err)
	victimSeen := dir + "/victim"
	survivorSeen := dir + "/survivor"
	marker := dir + "/killed"
	need := dir + "/need"
	must(os.WriteFile(victimSeen, nil, 0o644))
	must(os.WriteFile(survivorSeen, nil, 0o644))
	victim := spawnMember("victim", protocol, victimSeen, marker, need)
	survivor := spawnMember("survivor", protocol, survivorSeen, marker, need)
	deadline := time.Now().Add(45 * time.Second)
	var snapshot map[string]bool
	for time.Now().Before(deadline) {
		victimValues := readValues(victimSeen)
		survivorValues := readValues(survivorSeen)
		if len(victimValues) > 0 && len(survivorValues) > 0 && !containsAll(survivorValues, victimValues) {
			snapshot = victimValues
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	check(snapshot != nil, protocol+": members never split the log")
	must(victim.Process.Kill())
	_, _ = victim.Process.Wait()
	var lines []string
	for value := range snapshot {
		lines = append(lines, value)
	}
	must(os.WriteFile(need, []byte(strings.Join(lines, "\n")+"\n"), 0o644))
	must(os.WriteFile(marker, []byte("killed\n"), 0o644))
	done := make(chan error, 1)
	go func() { done <- survivor.Wait() }()
	select {
	case err := <-done:
		must(err)
	case <-time.After(45 * time.Second):
		_ = survivor.Process.Kill()
		fatal(fmt.Errorf("%s: survivor did not finish", protocol))
	}
}

func spawnMember(role, protocol, seen, marker, need string) *exec.Cmd {
	exe, err := os.Executable()
	must(err)
	cmd := exec.Command(exe, "member", strings.Join(brokers, ","), topic, group, role, protocol, seen, marker, need)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	must(cmd.Start())
	return cmd
}

func goMember() {
	brokers = strings.Split(os.Args[2], ",")
	topic = os.Args[3]
	group = os.Args[4]
	role := os.Args[5]
	protocol := os.Args[6]
	seen := os.Args[7]
	marker := os.Args[8]
	need := os.Args[9]
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(role),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}
	if protocol == "consumer" {
		opts = append(opts, kgo.ServerSideBalancer())
	} else {
		opts = append(opts, kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second))
	}
	client, err := kgo.NewClient(opts...)
	must(err)
	defer client.Close()
	got := map[string]bool{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fetches := client.PollFetches(ctx)
		cancel()
		for _, record := range fetches.Records() {
			value := string(record.Value)
			if got[value] {
				continue
			}
			got[value] = true
			file, err := os.OpenFile(seen, os.O_APPEND|os.O_WRONLY, 0o644)
			must(err)
			_, err = file.WriteString(value + "\n")
			must(err)
			must(file.Close())
		}
		if role == "survivor" {
			if _, err := os.Stat(marker); err == nil && containsAll(got, readValues(need)) && len(got) >= 40 {
				return
			}
		}
	}
	fatal(fmt.Errorf("%s %s timed out with %d", role, protocol, len(got)))
}

func producePinned(ctx context.Context, name string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	must(err)
	defer client.Close()
	for index := 0; index < 40; index++ {
		record := &kgo.Record{
			Topic:     name,
			Partition: int32(index % 4),
			Key:       []byte(strconv.Itoa(index)),
			Value:     []byte("v" + strconv.Itoa(index)),
		}
		must(client.ProduceSync(ctx, record).FirstErr())
	}
}

func readValues(path string) map[string]bool {
	text, err := os.ReadFile(path)
	if err != nil {
		return map[string]bool{}
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(text), "\n") {
		if strings.HasPrefix(line, "v") {
			out[line] = true
		}
	}
	return out
}

func containsAll(have, want map[string]bool) bool {
	for key := range want {
		if !have[key] {
			return false
		}
	}
	return true
}
