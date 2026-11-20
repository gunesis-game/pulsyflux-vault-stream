# Steering Guide — pulsyflux-vault-stream

Read this file first when continuing work in a new session. It records the agreed architecture, constraints, current direction, and unresolved decisions.

## Repositories and dependency

- Vault repository: https://github.com/gunesis-game/pulsyflux-vault-stream
- Generic stream repository: https://github.com/gunesis-game/pulsyflux-stream
- Related authentication repository: https://github.com/gunesis-game/pulsyflux-auth-stream
- The vault package owns the in-memory token store and typed vault request/response events.
- `pulsyflux-stream` owns generic typed event delivery. Do not duplicate its transport or registry.
- Inspect the actual pinned dependency version and current `pulsyflux-stream` API before coding. Do not rely on old API assumptions or guess method signatures.

## Product scope

- Keep this repository a small event-driven vault component.
- The current implementation is an in-memory key-to-token map.
- Do not add a direct HashiCorp Vault SDK, network client, database, persistence layer, encryption scheme, expiry policy, authorization system, or unrelated authentication mechanisms unless explicitly requested.
- The authentication component communicates with this component through typed request and response events. Do not introduce direct dependencies on authentication internals.

## Public API and ownership

The current public API is:

- `New(ctx context.Context, channel *stream.EventChannel) (*Vault, error)`
- `(*Vault).Close()`
- Typed event payloads: `StoreRequest`, `LookupRequest`, `Stored`, `Found`, and `NotFound`.
- Typed stream fields on `Vault`: `StoreRequests`, `LookupRequests`, `StoredEvents`, `FoundEvents`, and `NotFoundEvents`.

The constructor registers the five vault-specific typed streams on the supplied shared `EventChannel` using `stream.New[T](channel, bufferSize)`. These event types must not already be registered on that channel. Do not silently create a replacement channel: callers must share the same in-process channel instance.

Do not close the shared `EventChannel` or its streams from `Vault.Close()`; other components may use them. Close only subscriptions and resources owned by the vault instance.

## Typed event contract

All communication is through typed domain events. Do not publish untyped strings or boolean decisions.

Request events:

- `StoreRequest`: `RequestID`, `Key`, and `Token`. Stores the token under the supplied key.
- `LookupRequest`: `RequestID` and `Key`. Requests the token associated with the key.

Response events:

- `Stored`: confirms processing of a store request and carries its `RequestID` and `Key`.
- `Found`: carries the matching `RequestID`, `Key`, and token.
- `NotFound`: carries the `RequestID` and `Key` when no value exists.

A request ID must be non-zero and a key must be non-empty. Callers must subscribe to the appropriate response stream before publishing requests and correlate responses by `RequestID`.

Never log raw tokens, include them in errors, or expose them in response types that do not require the token. Do not treat a token as valid merely because it was found; authentication/verification policy belongs to the authentication component and its agreed contract.

## Event processing and lifecycle

- `New(ctx, channel)` registers the typed streams, subscribes to store and lookup request streams, and starts the processing loops.
- Store requests update the in-memory map under a mutex and publish a `Stored` response.
- Lookup requests read the map under a read lock and publish either `Found` or `NotFound`.
- Respect context cancellation, closed subscription channels, and subscription cleanup.
- `Close()` must be safe for a nil receiver and stop the owned processing loops without closing shared streams.
- Do not add callbacks, function-valued handlers, generic dispatchers, central type switches, or public helper APIs without explicit direction.
- Handle and surface publish failures deliberately. Do not silently lose response events in future revisions; consider a clear error-reporting/lifecycle strategy without changing the event contract unexpectedly.
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
3. Preserve the typed request/response contract and request correlation.
4. Add tests for storing, successful lookup, missing lookup, invalid requests, concurrent access, cancellation, closed streams, publish failures, and cleanup as appropriate.
5. Run `gofmt`, `go test -race ./...`, `go vet ./...`, and `go build ./...`.
6. Verify exported symbols and package documentation. Do not claim tests pass unless they have actually run successfully.
7. Keep README and this steering guide synchronized when architecture or public API changes.
8. Do not modify `pulsyflux-stream` or `pulsyflux-auth-stream` unless explicitly requested.
9. Do not create a release tag or publish a release unless explicitly requested.

## Collaboration preferences

- Keep changes focused and follow the owner's Go design and corrections literally.
- Prefer typed domain events and explicit request correlation.
- Avoid callbacks, anonymous callback handlers, generic handler layers, or unrelated abstractions.
- Ask one concise clarification only when a material contract cannot be determined from repository code or this guide.
- Never claim code was tested, committed, pushed, or released unless the action succeeded and was verified.
