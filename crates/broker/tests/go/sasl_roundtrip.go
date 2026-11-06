// franz-go: create a topic, produce, and fetch the same bytes for each codec.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: sasl_roundtrip bootstrap cert.pem")
		os.Exit(2)
	}
	pem, err := os.ReadFile(os.Args[2])
	if err != nil {
		fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		fatal(fmt.Errorf("no certificates in %s", os.Args[2]))
	}
	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	codecs := []struct {
		name string
		comp kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
	for _, codec := range codecs {
		topic := fmt.Sprintf("g%s%d", codec.name, time.Now().UnixNano())
		client, err := kgo.NewClient(
			kgo.SeedBrokers(os.Args[1]),
			kgo.SASL(plain.Auth{User: "camus", Pass: "camus-secret"}.AsMechanism()),
			kgo.DialTLSConfig(tlsConfig),
			kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchCompression(codec.comp),
			kgo.RequestTimeoutOverhead(8*time.Second),
		)
		if err != nil {
			fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		admin := kadm.NewClient(client)
		created, err := admin.CreateTopic(ctx, 1, 1, nil, topic)
		if err != nil {
			cancel()
			client.Close()
			fatal(err)
		}
		if created.Err != nil {
			cancel()
			client.Close()
			fatal(created.Err)
		}
		value := []byte(codec.name)
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: value}).FirstErr(); err != nil {
			cancel()
			client.Close()
			fatal(fmt.Errorf("%s produce: %w", codec.name, err))
		}
		client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		})
		found := false
		for !found {
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				cancel()
				client.Close()
				fatal(fmt.Errorf("%s fetch: %v", codec.name, errs))
			}
			fetches.EachRecord(func(record *kgo.Record) {
				if string(record.Value) == codec.name {
					found = true
				}
			})
		}
		cancel()
		client.Close()
		fmt.Printf("ok %s\n", codec.name)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
