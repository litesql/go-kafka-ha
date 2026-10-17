# go-kafka-ha

Kafka transport for SQLite replication built on top of `go-ha`.

`go-kafka-ha` provides the Kafka-backed publisher and subscriber needed to replicate SQLite changes across nodes using the `go-ha` replication model. It is designed for applications that already use `database/sql` with SQLite and want to send change sets through Kafka as the replication transport.

## What this project does

This library implements two pieces used by `go-ha`:

- `Producer`: serializes a `ha.ChangeSet` and publishes it to a Kafka topic
- `Consumer`: reads Kafka records, applies the change set to a local SQLite database, and commits offsets


## Features

- Kafka-backed replication publisher for `go-ha`
- Kafka consumer that applies replication messages to SQLite
- configurable Kafka seed brokers and topic
- SASL support with `plain`, `sha256`, and `sha512`
- optional insecure TLS for local testing and self-signed cert environments
- delivery policy controls for beginning, end, or committed offsets

## Installation

```bash
go get github.com/litesql/go-kafka-ha
```

## Quick start

The most common pattern is:

1. Create a Kafka producer and consumer
2. Wire them into a `sqliteha` connector with `go-ha`
3. Use the resulting `*sql.DB` as normal

```go
package main

import (
    "database/sql"
    "time"

    "github.com/litesql/go-ha"
    sqliteha "github.com/litesql/go-sqlite-ha"
    kafkaha "github.com/litesql/go-kafka-ha"
)

func main() {
    cfg := kafkaha.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "sqlite-replication",
    }

    producer, err := kafkaha.NewProducer(cfg, 15*time.Second)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    consumer, err := kafkaha.NewConsumer(cfg, kafkaha.DeliveryPolicyEarliest)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    connector, err := sqliteha.NewConnector(
        "example.db",
        ha.WithName("instance1"),
        ha.WithReplicationID("example"),
        ha.WithReplicationPublisher(producer),
        ha.WithReplicationSubscriber(consumer),
    )
    if err != nil {
        panic(err)
    }
    defer connector.Close()

    db := sql.OpenDB(connector)
    defer db.Close()

    if _, err := db.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
        panic(err)
    }

    if _, err := db.Exec("INSERT INTO users (name) VALUES (?)", "Alice"); err != nil {
        panic(err)
    }
}
```

## Configuration

```go
cfg := kafkaha.Config{
    Brokers:  []string{"kafka-1:9092", "kafka-2:9092"},
    Topic:    "sqlite-replication",
    SaslType: "plain", // "plain", "sha256", or "sha512"
    SaslUser: "replicator",
    SaslPass: "secret",
    Insecure: true,
}
```

### Config fields

- `Brokers`: list of Kafka bootstrap brokers
- `Topic`: Kafka topic used for replication messages
- `SaslType`: optional SASL mechanism (`plain`, `sha256`, `sha512`)
- `SaslUser`: SASL username
- `SaslPass`: SASL password
- `Insecure`: when `true`, skips TLS certificate verification for local dev setups

## Consumer delivery policies

`NewConsumer` accepts a `DeliveryPolicy`:

- `DeliveryPolicyDefault`: use the Kafka client default behavior
- `DeliveryPolicyEarliest`: start reading from the beginning of the topic
- `DeliveryPolicyLatest`: start reading from the end of the topic
- `DeliveryPolicyNone`: use committed offsets only

Use `DeliveryPolicyEarliest` when bringing up a fresh node that should replay the replication stream from the start.

## Typical architecture

```text
+------------------+      Kafka topic      +------------------+
| SQLite node A    | --------------------> | SQLite node B    |
| go-ha + Kafka    |                      | go-ha + Kafka    |
| producer         |                      | consumer         |
+------------------+                      +------------------+
```

Each node publishes local SQLite change sets to the same Kafka topic. Consumers apply the same changes on their local database, allowing replication across nodes while preserving the `database/sql` API.


## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

