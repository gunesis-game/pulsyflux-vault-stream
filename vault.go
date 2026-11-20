// Package vault implements a small in-memory token vault over pulsyflux-stream.
package vault

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gunesis-game/pulsyflux-stream"
)

const subscriberBuffer = 32

var (
	ErrNilEventChannel = errors.New("vault: nil event channel")
	ErrInvalidRequest  = errors.New("vault: request ID and key are required")
)

type StoreRequest struct {
	RequestID uuid.UUID
	Key       string
	Token     string
}

type LookupRequest struct {
	RequestID uuid.UUID
	Key       string
}

type Stored struct {
	RequestID uuid.UUID
	Key       string
}

type Found struct {
	RequestID uuid.UUID
	Key       string
	Token     string
}

type NotFound struct {
	RequestID uuid.UUID
	Key       string
}

// Vault owns an in-memory key/token map. The shared EventChannel is not closed by Vault.Close.
type Vault struct {
	StoreRequests  *stream.Stream[StoreRequest]
	LookupRequests *stream.Stream[LookupRequest]
	StoredEvents   *stream.Stream[Stored]
	FoundEvents    *stream.Stream[Found]
	NotFoundEvents *stream.Stream[NotFound]

	mu     sync.RWMutex
	values map[string]string
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New registers five typed streams on channel and starts the request handlers.
// Those event types must not already be registered on the supplied channel.
func New(ctx context.Context, channel *stream.EventChannel) (*Vault, error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	storeRequests, err := stream.New[StoreRequest](channel, subscriberBuffer)
	if err != nil { cancel(); return nil, err }
	lookupRequests, err := stream.New[LookupRequest](channel, subscriberBuffer)
	if err != nil { cancel(); return nil, err }
	storedEvents, err := stream.New[Stored](channel, subscriberBuffer)
	if err != nil { cancel(); return nil, err }
	foundEvents, err := stream.New[Found](channel, subscriberBuffer)
	if err != nil { cancel(); return nil, err }
	notFoundEvents, err := stream.New[NotFound](channel, subscriberBuffer)
	if err != nil { cancel(); return nil, err }

	v := &Vault{
		StoreRequests: storeRequests, LookupRequests: lookupRequests,
		StoredEvents: storedEvents, FoundEvents: foundEvents,
		NotFoundEvents: notFoundEvents, values: make(map[string]string), cancel: cancel,
	}
	storeSub, err := storeRequests.Subscribe(ctx)
	if err != nil { cancel(); return nil, err }
	lookupSub, err := lookupRequests.Subscribe(ctx)
	if err != nil { storeSub.Close(); cancel(); return nil, err }

	v.wg.Add(2)
	go v.handleStores(ctx, storeSub)
	go v.handleLookups(ctx, lookupSub)
	return v, nil
}

func (v *Vault) handleStores(ctx context.Context, sub *stream.Subscription[StoreRequest]) {
	defer v.wg.Done()
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.Events():
			if !ok { return }
			req := event.Data
			if req.RequestID == uuid.Nil || strings.TrimSpace(req.Key) == "" { continue }
			v.mu.Lock()
			v.values[req.Key] = req.Token
			v.mu.Unlock()
			_ = v.StoredEvents.Publish(stream.Event[Stored]{Name: "vault.stored", Data: Stored{RequestID: req.RequestID, Key: req.Key}})
		}
	}
}

func (v *Vault) handleLookups(ctx context.Context, sub *stream.Subscription[LookupRequest]) {
	defer v.wg.Done()
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.Events():
			if !ok { return }
			req := event.Data
			if req.RequestID == uuid.Nil || strings.TrimSpace(req.Key) == "" { continue }
			v.mu.RLock()
			token, found := v.values[req.Key]
			v.mu.RUnlock()
			if found {
				_ = v.FoundEvents.Publish(stream.Event[Found]{Name: "vault.found", Data: Found{RequestID: req.RequestID, Key: req.Key, Token: token}})
			} else {
				_ = v.NotFoundEvents.Publish(stream.Event[NotFound]{Name: "vault.not_found", Data: NotFound{RequestID: req.RequestID, Key: req.Key}})
			}
		}
	}
}

// Close stops the handlers without closing streams owned by the shared channel.
func (v *Vault) Close() {
	if v == nil { return }
	v.cancel()
	v.wg.Wait()
}
