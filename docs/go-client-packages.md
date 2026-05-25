# Go Client Packages

ech0 exposes two Go client layers:

- Embedded clients for applications that run a broker in-process.
- Remote TCP clients for applications that connect to a broker process.

The root package remains the embedded broker API. Role-focused embedded packages keep imports narrow when the application already owns a `*ech0.Broker`.

| Package | Role |
| --- | --- |
| `github.com/lyonbrown4d/ech0/admin` | Topic creation, topic summaries, health, quota, and stream metrics views. |
| `github.com/lyonbrown4d/ech0/producer` | Async idempotent producer wrapper around `Broker.NewProducer`. |
| `github.com/lyonbrown4d/ech0/consumer` | Direct consumer wrapper for fetch, ack, commit, seek, pause, resume, and nack. |
| `github.com/lyonbrown4d/ech0/txproducer` | Transactional producer wrapper for publish, offset staging, commit, and abort. |

These embedded packages do not create a second runtime model. They delegate to the same embedded broker APIs.

## Embedded Example

```go
ctx := context.Background()

broker, err := ech0.Open(ctx, ech0.DefaultOptions())
if err != nil {
    return err
}
defer broker.Close(ctx)

adminClient, err := admin.New(broker)
if err != nil {
    return err
}
if err := adminClient.CreateTopic(ctx, "orders", ech0.Partitions(3), ech0.OrderByKey(), ech0.PriorityRange(0, 9, 0)); err != nil {
    return err
}

producerClient, err := producer.New(ctx, broker, "orders", producer.BatchSize(32))
if err != nil {
    return err
}
defer producerClient.Close(ctx)

future, err := producerClient.Send(ctx, []byte(`{"id":"o-1"}`), ech0.Key([]byte("o-1")), ech0.Priority(7))
if err != nil {
    return err
}
if _, err := future.Await(ctx); err != nil {
    return err
}

consumerClient, err := consumer.New(broker, "orders-worker", "orders")
if err != nil {
    return err
}
fetched, err := consumerClient.Fetch(ctx, ech0.FetchLimit(10), ech0.ReadCommitted())
if err != nil {
    return err
}
for _, msg := range fetched.Messages {
    if err := consumerClient.Ack(ctx, msg); err != nil {
        return err
    }
}
```

Transactional publishing uses the same broker instance:

```go
tx, err := txproducer.New(ctx, broker, "orders-tx")
if err != nil {
    return err
}
if _, err := tx.Publish(ctx, "orders", []byte("created")); err != nil {
    return err
}
return tx.Commit(ctx)
```

## Remote TCP SDK

The `github.com/lyonbrown4d/ech0/client` package is the remote SDK. It uses `arcgolabs/clientx/tcp` for TCP dialing and transport policy hooks, then keeps ech0 framing and body codecs on `transport` plus `protocol/binary`. It owns connection pooling, per-connection handshake, auth metadata, tenant/namespace identity, capability negotiation, frame round-trips, and broker error decoding.

```go
ctx := context.Background()

mq, err := client.Dial(ctx, "127.0.0.1:9090",
    client.WithClientID("orders-api-1"),
    client.WithAuthToken("secret"),
    client.WithTenant("tenant-a"),
    client.WithNamespace("payments"),
)
if err != nil {
    return err
}
defer mq.Close(ctx)

if _, err := mq.Admin().CreateTopic(ctx, "orders", client.TopicPartitions(3)); err != nil {
    return err
}

producer := mq.Producer("orders", client.ProducerPartition(0))
if _, err := producer.Produce(ctx, []byte(`{"id":"o-1"}`), client.Key([]byte("o-1"))); err != nil {
    return err
}

consumer := mq.Consumer("orders-worker", "orders", client.ConsumerFetchLimit(10))
fetched, err := consumer.Fetch(ctx, client.FetchReadCommitted())
if err != nil {
    return err
}
for _, record := range fetched.Records {
    if _, err := consumer.Ack(ctx, record); err != nil {
        return err
    }
}
```

Transactional publishing over TCP uses the same remote client:

```go
tx, err := mq.BeginTransaction(ctx, "orders-tx")
if err != nil {
    return err
}
if _, err := tx.Publish(ctx, "orders", []byte("created"), client.Partition(0)); err != nil {
    _, _ = tx.Abort(ctx)
    return err
}
_, err = tx.Commit(ctx)
return err
```

Request/reply is exposed without requiring callers to build protocol envelopes:

```go
requester := mq.RequestReply("orders-api-1")
reply, err := requester.Request(ctx, "svc.price", []byte(`{"sku":"sku-1"}`),
    client.RequestTimeout(time.Second),
)
if err != nil {
    return err
}
_ = reply.Payload
```
