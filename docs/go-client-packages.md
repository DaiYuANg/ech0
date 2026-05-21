# Go Client Packages

ech0 keeps the root package as the embedded broker API, then exposes role-focused client packages for applications that want narrower imports.

| Package | Role |
| --- | --- |
| `github.com/lyonbrown4d/ech0/admin` | Topic creation, topic summaries, health, quota, and stream metrics views. |
| `github.com/lyonbrown4d/ech0/producer` | Async idempotent producer wrapper around `Broker.NewProducer`. |
| `github.com/lyonbrown4d/ech0/consumer` | Direct consumer wrapper for fetch, ack, commit, seek, pause, resume, and nack. |
| `github.com/lyonbrown4d/ech0/txproducer` | Transactional producer wrapper for publish, offset staging, commit, and abort. |

The packages do not create a second runtime model. They take an existing `*ech0.Broker` and delegate to the same embedded broker APIs.

## Example

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
