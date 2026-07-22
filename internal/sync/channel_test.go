package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestFiberChannelRendezvousPreservesValue(t *testing.T) {
	ch := NewFiberChannel(0)
	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "receiver")
	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	result := make(chan interface{}, 1)
	errCh := make(chan error, 1)
	go func() {
		value, err := ch.Receive(receiver)
		if err != nil {
			errCh <- err
			return
		}
		result <- value
	}()
	deadline := time.After(time.Second)
	for ch.RecvQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("receiver did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := ch.Send("payload", sender); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-result:
		if value != "payload" {
			t.Fatalf("received %v, want payload", value)
		}
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("receive did not complete")
	}
}

func TestFiberChannelCloseWakesWaiters(t *testing.T) {
	ch := NewFiberChannel(0)
	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "receiver")
	done := make(chan error, 1)
	go func() { _, err := ch.Receive(receiver); done <- err }()
	for ch.RecvQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	ch.Close()
	if !errors.Is(<-done, ErrChannelClosed) {
		t.Fatal("closed channel did not wake receiver with ErrChannelClosed")
	}
}

func TestFiberChannelNegativeCapacityIsUnbuffered(t *testing.T) {
	if got := NewFiberChannel(-1).Cap(); got != 0 {
		t.Fatalf("capacity = %d, want 0", got)
	}
}

func TestFiberChannelSendCtxCancelRemovesWaiter(t *testing.T) {
	ch := NewFiberChannel(0)
	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- ch.SendCtx(ctx, "payload", sender) }()
	deadline := time.After(time.Second)
	for ch.SendQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("sender did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("SendCtx did not return on cancel")
	}
	for ch.SendQueueSize() != 0 {
		time.Sleep(time.Millisecond)
	}
}
