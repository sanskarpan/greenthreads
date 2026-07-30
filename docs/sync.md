# Sync Primitives

greenthreads ships fiber-aware sync primitives in `internal/sync`. All
blocking operations require an explicit `*fiber.Fiber` argument — passing `nil`
panics on Lock/Wait and returns an error on Send.

Obtain the fiber pointer from inside the running fiber via
`rt.GetFiberDirect(fiberID)`.

---

## FiberMutex

Mutual exclusion lock. A fiber that calls `Lock` while the mutex is held is
parked in a wait-list and does not consume CPU. When the holder calls
`Unlock`, the first waiting fiber is re-queued into the scheduler.

```go
var mu sync.FiberMutex

mu.Lock(currentFiber)
// critical section
mu.Unlock()
```

- **Wait-list ordering:** FIFO — waiters are unblocked in arrival order.
- **Allocs on unblock:** 0 — the zero-alloc path re-uses the existing fiber struct.
- **Use case:** protecting shared state accessed by multiple fibers.

---

## FiberRWMutex

Reader/writer lock. Multiple concurrent readers are allowed; a writer blocks
until all readers release and no new readers are admitted.

```go
var rw sync.FiberRWMutex

// reader
rw.RLock(f)
val := sharedMap[key]
rw.RUnlock()

// writer
rw.Lock(f)
sharedMap[key] = newVal
rw.Unlock()
```

- **Use case:** read-heavy shared state — caches, config, routing tables.

---

## FiberChannel

Unbuffered or buffered channel between fibers. A sender on a full channel and a
receiver on an empty channel are both parked until the opposite side is ready.

```go
ch := sync.NewFiberChannel(8) // buffered capacity 8

// producer fiber
ch.Send(f, item)

// consumer fiber
item, ok := ch.Receive(f)
```

---

## FiberChannelOf[T]

Generic typed wrapper around `FiberChannel`. Eliminates the need for type
assertions on the receiving side.

```go
ch := sync.NewFiberChannelOf[int](8)

ch.Send(f, 42)

v, ok := ch.Receive(f) // v is int
```

---

## FiberWaitGroup

Barrier: wait until N fibers signal Done.

```go
var wg sync.FiberWaitGroup
wg.Add(10)

for i := 0; i < 10; i++ {
    i := i
    rt.Spawn(func() {
        process(i)
        wg.Done()
    }, fmt.Sprintf("worker-%d", i))
}

wg.Wait(coordinatorFiber)
```

- **Use case:** fan-out coordination inside the fiber graph.

---

## FiberSemaphore

Counting semaphore limiting concurrent access to a resource.

```go
sem := sync.NewFiberSemaphore(3) // max 3 concurrent holders

sem.Acquire(f)
// use limited resource
sem.Release()
```

- **Use case:** connection pools, rate limiting within fibers, limiting
  concurrent writes to a shared buffer.

---

## SpawnGroup

Structured fan-out with automatic error collection. Preferred over manually
coordinating `FiberWaitGroup` when you need to collect errors from workers.

```go
sg := rt.NewSpawnGroup()

for i := 0; i < 10; i++ {
    i := i
    sg.Spawn(func() {
        if err := process(i); err != nil {
            // returning an error is not yet directly supported;
            // use a shared slice + FiberMutex or SpawnWithResult instead
        }
    }, fmt.Sprintf("worker-%d", i))
}

// Block until all fibers in the group finish.
errs := sg.Wait()
for _, err := range errs {
    if err != nil {
        log.Printf("worker error: %v", err)
    }
}
```

---

## Deadlock detector

The deadlock detector runs as a background goroutine, periodically scanning the
wait-graph for cycles. It does not prevent deadlocks — it surfaces them.

```go
rt.DeadlockDetector().SetEnabled(true)

// later, after a suspected deadlock:
deadlocks := rt.DeadlockDetector().GetDeadlocks()
for _, d := range deadlocks {
    log.Printf("deadlock: fiber %d waiting on fiber %d", d.WaiterID, d.HolderID)
}

rt.DeadlockDetector().ClearDeadlocks()
```

!!! warning "Slot exhaustion deadlock"
    The most common deadlock in greenthreads is **slot exhaustion**: all
    `numWorkers` slots are held by fibers blocked on primitives that can only
    be resolved by fibers waiting in the queue.

    **Fix:** ensure `numWorkers > max_simultaneously_blocked_fibers`.
