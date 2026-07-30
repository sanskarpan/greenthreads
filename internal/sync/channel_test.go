package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func TestFiberChannelRendezvousPreservesValue(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	if got := NewFiberChannel(-1).Cap(); got != 0 {
		t.Fatalf("capacity = %d, want 0", got)
	}
}

func TestFiberChannelSendCtxCancelRemovesWaiter(t *testing.T) {
	t.Parallel()
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

func TestFiberChannelTrySend(t *testing.T) {
	t.Parallel()

	// nil channel
	var nilCh *FiberChannel
	if ok, _ := nilCh.TrySend("x"); ok {
		t.Fatal("nil TrySend should return false")
	}

	// closed channel
	ch := NewFiberChannel(1)
	ch.Close()
	if ok, err := ch.TrySend("x"); ok || err != ErrChannelClosed {
		t.Fatal("TrySend on closed channel should return false, ErrChannelClosed")
	}

	// buffered - success
	ch2 := NewFiberChannel(1)
	if ok, err := ch2.TrySend("a"); !ok || err != nil {
		t.Fatal("TrySend on buffered channel should succeed")
	}

	// buffer full - failure
	if ok, err := ch2.TrySend("b"); ok || err != nil {
		t.Fatal("TrySend on full buffer should fail")
	}

	// rendezvous with waiting receiver
	ch3 := NewFiberChannel(0)
	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "recv")
	errCh := make(chan error, 1)
	valCh := make(chan interface{}, 1)
	go func() {
		v, err := ch3.Receive(receiver)
		valCh <- v
		errCh <- err
	}()
	for ch3.RecvQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	if ok, err := ch3.TrySend("direct"); !ok || err != nil {
		t.Fatal("TrySend to waiting receiver should succeed")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if v := <-valCh; v != "direct" {
		t.Fatalf("received %v, want direct", v)
	}
}

func TestFiberChannelTryReceive(t *testing.T) {
	t.Parallel()

	// nil channel
	var nilCh *FiberChannel
	if v, ok := nilCh.TryReceive(); ok || v != nil {
		t.Fatal("nil TryReceive should return nil, false")
	}

	// nothing available
	ch := NewFiberChannel(1)
	if v, ok := ch.TryReceive(); ok || v != nil {
		t.Fatal("TryReceive on empty channel should return nil, false")
	}

	// from buffer
	_, _ = ch.TrySend("buffered")
	v, ok := ch.TryReceive()
	if !ok || v != "buffered" {
		t.Fatalf("TryReceive got %v, %v, want buffered, true", v, ok)
	}

	// from sender handoff
	ch2 := NewFiberChannel(0)
	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	go func() { _ = ch2.Send("handoff", sender) }()
	for ch2.SendQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	v, ok = ch2.TryReceive()
	if !ok || v != "handoff" {
		t.Fatalf("TryReceive from sender handoff got %v, %v, want handoff, true", v, ok)
	}
}

func TestFiberChannelIsClosed(t *testing.T) {
	t.Parallel()

	// nil
	var nilCh *FiberChannel
	if !nilCh.IsClosed() {
		t.Fatal("nil channel IsClosed should return true")
	}

	ch := NewFiberChannel(1)
	if ch.IsClosed() {
		t.Fatal("new channel should not be closed")
	}
	ch.Close()
	if !ch.IsClosed() {
		t.Fatal("closed channel should report closed")
	}
}

func TestFiberChannelLen(t *testing.T) {
	t.Parallel()

	// nil
	var nilCh *FiberChannel
	if nilCh.Len() != 0 {
		t.Fatal("nil channel Len should be 0")
	}

	ch := NewFiberChannel(3)
	if ch.Len() != 0 {
		t.Fatal("empty channel Len should be 0")
	}
	_, _ = ch.TrySend("a")
	_, _ = ch.TrySend("b")
	if ch.Len() != 2 {
		t.Fatalf("Len after 2 sends = %d, want 2", ch.Len())
	}
}

func TestFiberChannelCap(t *testing.T) {
	t.Parallel()

	// nil
	var nilCh *FiberChannel
	if nilCh.Cap() != 0 {
		t.Fatal("nil channel Cap should be 0")
	}
	// normal
	ch := NewFiberChannel(5)
	if ch.Cap() != 5 {
		t.Fatalf("Cap = %d, want 5", ch.Cap())
	}
}

func TestFiberChannelSendQueueSize(t *testing.T) {
	t.Parallel()

	// nil
	var nilCh *FiberChannel
	if nilCh.SendQueueSize() != 0 {
		t.Fatal("nil SendQueueSize should be 0")
	}

	ch := NewFiberChannel(0)
	if ch.SendQueueSize() != 0 {
		t.Fatal("new channel SendQueueSize should be 0")
	}

	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	go func() { _ = ch.Send("blocked", sender) }()
	for ch.SendQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	if ch.SendQueueSize() != 1 {
		t.Fatalf("SendQueueSize = %d, want 1", ch.SendQueueSize())
	}
	// Drain the sender so the goroutine doesn't leak
	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "recv")
	_, _ = ch.Receive(receiver)
}

func TestFiberChannelRecvQueueSize(t *testing.T) {
	t.Parallel()

	// nil
	var nilCh *FiberChannel
	if nilCh.RecvQueueSize() != 0 {
		t.Fatal("nil RecvQueueSize should be 0")
	}

	ch := NewFiberChannel(0)
	if ch.RecvQueueSize() != 0 {
		t.Fatal("new channel RecvQueueSize should be 0")
	}

	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "recv")
	go func() { _, _ = ch.Receive(receiver) }()
	for ch.RecvQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	if ch.RecvQueueSize() != 1 {
		t.Fatalf("RecvQueueSize = %d, want 1", ch.RecvQueueSize())
	}
	// Unblock the receiver
	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	_ = ch.Send("unblock", sender)
}

func TestFiberChannelAdmitSenderLocked(t *testing.T) {
	t.Parallel()

	// Create a full buffered channel with a blocked sender.
	// Receiving from the buffer should admit the sender.
	ch := NewFiberChannel(1)
	_, _ = ch.TrySend("fill")

	sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
	go func() { _ = ch.Send("admitted", sender) }()
	for ch.SendQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}

	// Receive from buffer — should admit the blocked sender into the buffer
	receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "recv")
	v, err := ch.Receive(receiver)
	if err != nil {
		t.Fatal(err)
	}
	if v != "fill" {
		t.Fatalf("received %v, want fill", v)
	}

	// The admitted sender's value should now be in the buffer
	if ch.Len() != 1 {
		t.Fatalf("buffer len = %d, want 1", ch.Len())
	}
	v, err = ch.Receive(receiver)
	if err != nil {
		t.Fatal(err)
	}
	if v != "admitted" {
		t.Fatalf("received %v, want admitted", v)
	}
}
