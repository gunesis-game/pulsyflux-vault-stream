# Steering Guide — pulsyflux-vault-stream

Read this file first when continuing work in a new session. It records the agreed architecture, constraints, current direction, and unresolved decisions.

## Repositories and dependency

- Vault repository: https://github.com/gunesis-game/pulsyflux-vault-stream
- Generic stream repository: https://github.com/gunesis-game/pulsyflux-stream
- Related authentication repository: https://github.com/gunesis-game/pulsyflux-auth-stream
- The vault package owns the in-memory token store and typed vault domain events.
- `pulsyflux-stream` owns generic typed event delivery. Do not duplicate its transport or registry.
- Inspect the actual pinned dependency version and current `pulsyflux-stream` API before coding.

## Product scope

- Keep this repository a small event-driven vault component.
- The current implementation is an in-memory key-to-token map.
- Do not add a direct HashiCorp Vault SDK, network client, database, persistence layer, encryption scheme, expiry policy, authorization system, or unrelated authentication mechanisms unless explicitly requested.
- Authentication communicates with this component through the typed vault events defined here. Do not introduce direct dependencies on authentication internals.

## Event naming convention — mandatory

Every domain payload used as `stream.Stream[T]` or `stream.Event[T]` MUST be a named event type with the `Event` suffix. Do not use generic nouns or request/result structs such as `StoreRequest`, `LookupRequest`, `Stored`, `Found`, or `NotFound` as stream payload types.

Use explicit domain names that describe the event and its direction. Keep the Go type name, stream field name, and event envelope name semantically aligned.

## Public API and event contract

The public API is:

- `New(ctx context.Context, channel *stream.EventChannel) (*Vault, error)`
- `(*Vault).Close()`
- Typed event payloads listed below.
- Typed stream fields on `Vault` listed below.

### Events published to the vault

- `VaultTokenStoreRequestedEvent`: `Key string`, `Token string`. Requests storing a token under a key.
- `VaultTokenRequestedEvent`: `Key string`. Requests looking up a token by key. This is the shared auth-to-vault lookup contract.

### Events published by the vault

- `VaultTokenStoredEvent`: `Key string`. Confirms the store operation.
- `VaultTokenFoundEvent`: `Key string`, `Token string`. Successful lookup response. This is consumed by auth.
- `VaultTokenNotFoundEvent`: `Key string`. Missing lookup response. This is consumed by auth.

The auth integration names are intentionally exact and must remain stable:
`VaultTokenRequestedEvent`, `VaultTokenFoundEvent`, and `VaultTokenNotFoundEvent`.
The auth and vault event streams are registered on the same shared `EventChannel`, which establishes their logical relationship. No per-request identifier is part of these event payloads. The Auth integration must be critically tested to verify that the shared-channel event relationship is sufficient for the intended workflow.

The key must be non-empty. Callers must subscribe to response streams before publishing requests. Never log raw tokens, include them in errors, or expose them in event payloads that do not require the token. A found token is not automatically an authenticated/valid token; verification policy belongs to auth.

### Stream ownership

The `Vault` exposes these typed streams:

- `VaultTokenStoreRequestedEvents`: input `Stream[VaultTokenStoreRequestedEvent]`.
- `VaultTokenStoredEvents`: output `Stream[VaultTokenStoredEvent]`.
- `VaultTokenRequestedEvents`: input `Stream[VaultTokenRequestedEvent]`.
- `VaultTokenFoundEvents`: output `Stream[VaultTokenFoundEvent]`.
- `VaultTokenNotFoundEvents`: output `Stream[VaultTokenNotFoundEvent]`.

The constructor registers the five event streams on the supplied shared `EventChannel` using `stream.New[T](channel, bufferSize)`. These event types must not already be registered on that channel. Do not silently create a replacement channel: all participants must share the same in-process channel instance.

Do not close the shared `EventChannel` or registered streams from `Vault.Close()`; other components may use them. Close only subscriptions and resources owned by this vault instance.

## Event processing and lifecycle

- `New(ctx, channel)` registers the typed domain-event streams, subscribes to store and lookup request streams, and starts processing loops.
- Store requests update the in-memory map under a mutex and publish `VaultTokenStoredEvent`.
- Lookup requests read the map under a read lock and publish exactly one of `VaultTokenFoundEvent` or `VaultTokenNotFoundEvent`.
- Respect context cancellation, closed subscription channels, and subscription cleanup.
- `Close()` must be safe for a nil receiver and stop owned processing loops without closing shared streams.
- Do not add callbacks, function-valued handlers, generic dispatchers, central type switches, or public helper APIs without explicit direction.
- Handle publish failures deliberately; do not silently lose response events in future revisions. Any error-reporting change must preserve the agreed event contract unless explicitly approved.
- Avoid goroutine leaks and races. Keep the map concurrency-safe.

## Storage and security limitations

The current implementation is intentionally in-memory:

- Values are lost when the process exits.
- There is no encryption at rest, persistence, expiry, authorization, secret redaction, or cross-process transport.
- All participants must share the same in-process `EventChannel`. A stable channel UUID does not make the transport distributed.
- Do not describe this package as a production secrets manager or claim production-grade security.

Any future addition of persistence, encryption, expiry, or distributed transport requires explicit product direction and documented threat-model decisions.

## Build, test, and release

- CI runs on pushes and pull requests targeting `main`.
- CI uses Go 1.27.x and runs `go mod tidy`, `go test -race ./...`, `go vet ./...`, and `go build ./...`.
- Releases are created from semantic version tags matching `v*.*.*` or through the release workflow's manual dispatch input.
- This repository is a Go module. A version tag is the module version consumed by Go tooling; the release workflow also creates a GitHub Release with generated release notes.
- Do not claim tests or workflows passed unless their runs have actually completed successfully.

## Required implementation and verification steps

1. Inspect the current branch, files, recent commits, and open pull requests before editing.
2. Inspect the pinned `pulsyflux-stream` dependency and verify `EventChannel`, stream registration, `Subscribe(ctx)`, subscription channels, event envelopes, and `Publish` signatures.
3. Preserve the event-suffixed type naming convention and the exact auth integration event names; do not add per-request identifiers.
4. Add tests for storing, successful lookup, missing lookup, invalid requests, concurrent access, cancellation, closed streams, publish failures, and cleanup as appropriate.
5. Run `gofmt`, `go test -race ./...`, `go vet ./...`, and `go build ./...`.
6. Verify exported symbols and package documentation. Do not claim tests pass unless they have actually run successfully.
7. Keep README and this steering guide synchronized when architecture or public API changes.
8. Do not modify `pulsyflux-stream` or `pulsyflux-auth-stream` unless explicitly requested.
9. Do not create a release tag or publish a release unless explicitly requested.

## Collaboration preferences

- Keep changes focused and follow the owner's Go design and corrections literally.
- Use typed, event-suffixed domain payloads and the shared EventChannel relationship between component event streams; do not add per-request correlation identifiers.
- Keep the auth-to-vault integration event names stable.
- Avoid callbacks, anonymous callback handlers, generic handler layers, and unrelated abstractions.
- Ask one concise clarification only when a material contract cannot be determined from repository code or this guide.
- Never claim code was tested, committed, pushed, or released unless the action succeeded and was verified.
