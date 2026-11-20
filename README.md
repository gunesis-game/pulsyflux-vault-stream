# pulsyflux-vault-stream

An in-memory token vault built on [pulsyflux-stream](https://github.com/gunesis-game/pulsyflux-stream).

The package communicates exclusively through typed domain events. Every payload type used as a `stream.Stream[T]` is named with the `Event` suffix.

## Authentication integration

The vault exposes the three event types required by
[pulsyflux-auth-stream](https://github.com/gunesis-game/pulsyflux-auth-stream):

- `VaultTokenRequestedEvent` — auth publishes this to request a lookup.
- `VaultTokenFoundEvent` — vault publishes this when the key exists; includes the token.
- `VaultTokenNotFoundEvent` — vault publishes this when the key does not exist.

All three carry a `RequestID` so auth can correlate concurrent requests and responses. They also carry `Key`; the found response additionally carries `Token`.

The vault also exposes its own store lifecycle events:

- `VaultTokenStoreRequestedEvent` — publish this to store a token under a key.
- `VaultTokenStoredEvent` — emitted after the in-memory store is updated.

## Usage

Create one shared `EventChannel` for the vault and auth components. Construct the vault on that channel, subscribe to result events, and publish requests:

```go
channel := stream.NewEventChannel()
vault, err := vaultstream.New(ctx, channel)
if err != nil {
    return err
}
defer vault.Close()

found, err := vault.VaultTokenFoundEvents.Subscribe(ctx)
if err != nil {
    return err
}

requestID := uuid.New()
err = vault.VaultTokenRequestedEvents.Publish(stream.Event[vaultstream.VaultTokenRequestedEvent]{
    Name: "vault.token.requested",
    Data: vaultstream.VaultTokenRequestedEvent{
        RequestID: requestID,
        Key:       "session-1",
    },
})
```

Subscribe to the response streams before publishing requests so responses are not missed. Match each response using `RequestID`.

## Scope and security

This is a minimal in-memory implementation. Values are lost when the process exits. It does not provide encryption at rest, persistence, expiry, authorization, secret redaction, or cross-process transport. Do not use it as a production secrets manager without adding those guarantees.

All participants must share the same in-process `EventChannel`. A stable event-channel ID does not make the transport distributed.
