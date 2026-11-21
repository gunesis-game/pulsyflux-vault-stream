package vault

import (
	"context"
	"testing"
	"time"

	"github.com/gunesis-game/pulsyflux-stream"
)

func makeEvent[T any](t *testing.T, data T) stream.Event[T] {
	t.Helper()
	event, err := stream.NewEvent[T](stream.NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(data); err != nil {
		t.Fatal(err)
	}
	return event
}

func TestStoreAndLookupEvents(t *testing.T) {
	channel := stream.NewEventChannel()
	v, err := New(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stored, err := v.VaultTokenStoredEvents.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stored.Close()

	found, err := v.VaultTokenFoundEvents.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer found.Close()
	if err := v.VaultTokenStoreRequestedEvents.Publish(makeEvent(t, VaultTokenStoreRequestedEvent{Key: "session-1", Token: "secret"})); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-stored.Events():
		if event.Data().Key != "session-1" {
			t.Fatalf("unexpected stored event: %+v", event.Data())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for stored event")
	}

	// This is the request/response contract used by pulsyflux-auth-stream.
	if err := v.VaultTokenRequestedEvents.Publish(makeEvent(t, VaultTokenRequestedEvent{Key: "session-1"})); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-found.Events():
		if event.Data().Key != "session-1" || event.Data().Token != "secret" {
			t.Fatalf("unexpected found event: %+v", event.Data())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for found event")
	}
}

func TestLookupMissingPublishesNotFoundEvent(t *testing.T) {
	channel := stream.NewEventChannel()
	v, err := New(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	notFound, err := v.VaultTokenNotFoundEvents.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer notFound.Close()
	if err := v.VaultTokenRequestedEvents.Publish(makeEvent(t, VaultTokenRequestedEvent{Key: "missing"})); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-notFound.Events():
		if event.Data().Key != "missing" {
			t.Fatalf("unexpected not-found event: %+v", event.Data())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for not-found event")
	}
}
