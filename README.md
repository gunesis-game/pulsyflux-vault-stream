# pulsyflux-vault-stream

A small in-memory token vault built on [pulsyflux-stream](https://github.com/gunesis-game/pulsyflux-stream).

## Usage

Create one shared event channel, construct the vault on it, and publish typed requests:

```go
channel := stream.NewEventChannel()
vault, err := vaultstream.New(ctx, channel)
if err != nil {
    return err
}
defer vault.Close()

stored, _ := vault.StoredEvents.Subscribe(ctx)
found, _ := vault.FoundEvents.Subscribe(ctx)

id := uuid.New()
_ = vault.StoreRequests.Publish(stream.Event[vaultstream.StoreRequest]{
    Name: "vault.store",
    Data: vaultstream.StoreRequest{RequestID: id, Key: "session-1", Token: "token-value"},
})
```

Subscribe to result streams before publishing requests so responses are not missed. Correlate responses using RequestID.

- StoreRequests accepts StoreRequest and emits StoredEvents.
- LookupRequests accepts LookupRequest and emits either FoundEvents or NotFoundEvents.

## Scope and security

This is a minimal in-memory implementation. Values are lost when the process exits. It does not provide encryption at rest, persistence, expiry, authorization, secret redaction, or cross-process transport. Do not use it as a production secrets manager without adding those guarantees.

A stable event-channel ID does not make the stream transport distributed; all participants must share the same in-process EventChannel.
