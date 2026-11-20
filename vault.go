// Package vault implements an in-memory token vault over pulsyflux-stream.
package vault

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/gunesis-game/pulsyflux-stream"
)

const subscriberBuffer = 32

var ErrNilEventChannel = errors.New("vault: nil event channel")

// VaultTokenStoreRequestedEvent asks the vault to store Token under Key.
type VaultTokenStoreRequestedEvent struct {
	Key       string
	Token     string
}

// VaultTokenStoredEvent confirms that a store request was processed.
type VaultTokenStoredEvent struct {
	Key       string
}

// VaultTokenRequestedEvent asks the vault to look up the token associated with Key.
// This is the request event consumed by pulsyflux-auth-stream.
type VaultTokenRequestedEvent struct {
	Key       string
}

// VaultTokenFoundEvent is the successful lookup response consumed by auth.
type VaultTokenFoundEvent struct {
	Key       string
	Token     string
}

// VaultTokenNotFoundEvent is emitted when a requested key is absent.
type VaultTokenNotFoundEvent struct {
	Key       string
}

// Vault owns an in-memory key/token map and its request subscriptions.
// Its EventChannel and registered streams are shared resources and are not
// closed by Vault.Close.
type Vault struct {
	VaultTokenStoreRequestedEvents *stream.Stream[VaultTokenStoreRequestedEvent]
	VaultTokenStoredEvents          *stream.Stream[VaultTokenStoredEvent]
	VaultTokenRequestedEvents       *stream.Stream[VaultTokenRequestedEvent]
	VaultTokenFoundEvents           *stream.Stream[VaultTokenFoundEvent]
	VaultTokenNotFoundEvents        *stream.Stream[VaultTokenNotFoundEvent]

	mu     sync.RWMutex
	values map[string]string
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New registers the vault's typed domain-event streams on channel and starts
// processing store and lookup requests. Each event type must be unregistered
// on the supplied channel before construction.
func New(ctx context.Context, channel *stream.EventChannel) (*Vault, error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	storeRequests, err := stream.New[VaultTokenStoreRequestedEvent](channel, subscriberBuffer)
	if err != nil {
		cancel()
		return nil, err
	}
	storedEvents, err := stream.New[VaultTokenStoredEvent](channel, subscriberBuffer)
	if err != nil {
		cancel()
		return nil, err
	}
	lookupRequests, err := stream.New[VaultTokenRequestedEvent](channel, subscriberBuffer)
	if err != nil {
		cancel()
		return nil, err
	}
	foundEvents, err := stream.New[VaultTokenFoundEvent](channel, subscriberBuffer)
	if err != nil {
		cancel()
		return nil, err
	}
	notFoundEvents, err := stream.New[VaultTokenNotFoundEvent](channel, subscriberBuffer)
	if err != nil {
		cancel()
		return nil, err
	}

	v := &Vault{
		VaultTokenStoreRequestedEvents: storeRequests,
		VaultTokenStoredEvents:          storedEvents,
		VaultTokenRequestedEvents:       lookupRequests,
		VaultTokenFoundEvents:           foundEvents,
		VaultTokenNotFoundEvents:        notFoundEvents,
		values:                          make(map[string]string),
		cancel:                          cancel,
	}

	storeSub, err := storeRequests.Subscribe(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	lookupSub, err := lookupRequests.Subscribe(ctx)
	if err != nil {
		storeSub.Close()
		cancel()
		return nil, err
	}

	v.wg.Add(2)
	go v.handleStoreRequests(ctx, storeSub)
	go v.handleLookupRequests(ctx, lookupSub)
	return v, nil
}

func (v *Vault) handleStoreRequests(ctx context.Context, sub *stream.Subscription[VaultTokenStoreRequestedEvent]) {
	defer v.wg.Done()
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.Events():
			if !ok {
				return
			}
			request := event.Data
			if strings.TrimSpace(request.Key) == "" {
				continue
			}

			v.mu.Lock()
			v.values[request.Key] = request.Token
			v.mu.Unlock()

			_ = v.VaultTokenStoredEvents.Publish(stream.Event[VaultTokenStoredEvent]{
				Name: "vault.token.stored",
				Data: VaultTokenStoredEvent{Key: request.Key},
			})
		}
	}
}

func (v *Vault) handleLookupRequests(ctx context.Context, sub *stream.Subscription[VaultTokenRequestedEvent]) {
	defer v.wg.Done()
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.Events():
			if !ok {
				return
			}
			request := event.Data
			if strings.TrimSpace(request.Key) == "" {
				continue
			}

			v.mu.RLock()
			token, found := v.values[request.Key]
			v.mu.RUnlock()

			if found {
				_ = v.VaultTokenFoundEvents.Publish(stream.Event[VaultTokenFoundEvent]{
					Name: "vault.token.found",
					Data: VaultTokenFoundEvent{Key: request.Key, Token: token},
				})
				continue
			}

			_ = v.VaultTokenNotFoundEvents.Publish(stream.Event[VaultTokenNotFoundEvent]{
				Name: "vault.token.not_found",
				Data: VaultTokenNotFoundEvent{Key: request.Key},
			})
		}
	}
}

// Close stops the vault's owned subscriptions and processing loops. It does
// not close the shared EventChannel or any registered streams.
func (v *Vault) Close() {
	if v == nil {
		return
	}
	v.cancel()
	v.wg.Wait()
}
