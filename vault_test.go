package vault

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gunesis-game/pulsyflux-stream"
)

func TestStoreAndLookup(t *testing.T) {
	channel := stream.NewEventChannel()
	v, err := New(context.Background(), channel)
	if err != nil { t.Fatal(err) }
	defer v.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stored, err := v.StoredEvents.Subscribe(ctx)
	if err != nil { t.Fatal(err) }
	defer stored.Close()
	found, err := v.FoundEvents.Subscribe(ctx)
	if err != nil { t.Fatal(err) }
	defer found.Close()

	storeID := uuid.New()
	if err := v.StoreRequests.Publish(stream.Event[StoreRequest]{
		Name: "vault.store", Data: StoreRequest{RequestID: storeID, Key: "session-1", Token: "secret"},
	}); err != nil { t.Fatal(err) }
	select {
	case event := <-stored.Events():
		if event.Data.RequestID != storeID || event.Data.Key != "session-1" { t.Fatalf("unexpected stored event: %+v", event.Data) }
	case <-ctx.Done():
		t.Fatal("timed out waiting for stored event")
	}

	lookupID := uuid.New()
	if err := v.LookupRequests.Publish(stream.Event[LookupRequest]{
		Name: "vault.lookup", Data: LookupRequest{RequestID: lookupID, Key: "session-1"},
	}); err != nil { t.Fatal(err) }
	select {
	case event := <-found.Events():
		if event.Data.RequestID != lookupID || event.Data.Token != "secret" { t.Fatalf("unexpected found event: %+v", event.Data) }
	case <-ctx.Done():
		t.Fatal("timed out waiting for found event")
	}
}
