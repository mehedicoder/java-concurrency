# Java Concurrency Decision Tree

Start from the shape of the problem, then pick the smallest concurrency tool that preserves correctness.

## First Question: What Are You Trying To Improve?

```text
Do you need concurrency at all?
|
+-- No, the code is fast enough and simpler sequentially
|   +-- Use sequential code.
|
+-- Yes, you are waiting on I/O
|   +-- Go to I/O-bound work.
|
+-- Yes, you are using CPU heavily
|   +-- Go to CPU-bound work.
|
+-- Yes, multiple threads touch shared state
|   +-- Go to shared mutable state.
|
+-- Yes, tasks must coordinate phases, limits, or ordering
|   +-- Go to coordination.
|
+-- Yes, tasks are part of one request and must succeed/fail together
    +-- Go to request-scoped concurrency.
```

## Quick Selection Table

| Problem shape | Prefer | Avoid as first choice | Example package |
| --- | --- | --- | --- |
| Many blocking I/O calls | Virtual threads, structured concurrency | Large platform-thread pools | `g_structured`, `f_async` |
| One request fans out to several services | Structured concurrency | Detached `CompletableFuture` chains | `g_structured` |
| Event/callback pipeline | `CompletableFuture`, reactive streams, message queues | Blocking joins inside callbacks | `f_async` |
| CPU-heavy independent work | Fixed-size executor, `ForkJoinPool`, parallel streams with care | One thread per task | `h_parallel`, `i_measurement` |
| Simple counter/flag/reference | `AtomicInteger`, `AtomicLong`, `AtomicReference`, `volatile` for visibility-only state | `synchronized` around a single atomic value | `b_mutual_exclusion`, `j_jmm` |
| Compound invariant over multiple fields | `synchronized`, `ReentrantLock` | Multiple independent atomics | `b_mutual_exclusion`, `c_locks` |
| Many readers, few writers | Immutable snapshot, copy-on-write, `ReadWriteLock`, `StampedLock` | Single global exclusive lock | `c_locks` |
| Producer-consumer handoff | `BlockingQueue`, executor, flow/reactive API | Shared list plus manual wait/notify | `e_synchronizers` |
| Limit access to scarce resource | `Semaphore`, bounded executor, rate limiter | Sleeping/retry loops | `e_synchronizers` |
| Wait until N tasks finish | `CountDownLatch`, structured scope join, `CompletableFuture.allOf` | Busy waiting | `e_synchronizers`, `g_structured` |
| Repeated phase barrier | `CyclicBarrier`, `Phaser` | Recreating latches in loops | `e_synchronizers` |
| Ultra-low-level ordering or lock-free algorithm | VarHandle, fences, jcstress tests | Hand-rolled memory tricks without tests | `j_jmm` |

## Decision Tree

### 1. I/O-Bound Work

```text
Is each task mostly blocking on network, disk, database, sleep, or external services?
|
+-- Yes
|   |
|   +-- Is the work tied to a request/user operation?
|   |   +-- Yes: use structured concurrency with virtual threads.
|   |   +-- No: use an executor or background queue with bounded lifecycle.
|   |
|   +-- Do you need to compose async callbacks without occupying threads?
|       +-- Yes: use CompletableFuture or a reactive/streaming API.
|       +-- No: prefer virtual threads for simpler blocking-style code.
|
+-- No
    +-- Go to CPU-bound work.
```

Use:

- `Executors.newVirtualThreadPerTaskExecutor()` for many blocking tasks.
- `StructuredTaskScope` when child tasks belong to the same parent operation and need cancellation/failure propagation.
- `CompletableFuture` when the API is naturally asynchronous or callback-based.

Rules of thumb:

- Virtual threads improve scalability for blocking I/O, not raw CPU throughput.
- Always put timeouts around remote calls.
- Bound the external dependency, not just the Java threads. Databases, APIs, and connection pools still have limits.

### 2. CPU-Bound Work

```text
Is the bottleneck CPU calculation?
|
+-- Yes
|   |
|   +-- Can the work be split into independent chunks?
|   |   +-- Yes: use a fixed-size pool, ForkJoinPool, or parallel stream.
|   |   +-- No: keep sequential or redesign the algorithm.
|   |
|   +-- Is each chunk large enough to offset scheduling overhead?
|       +-- Yes: parallelize.
|       +-- No: batch work or keep sequential.
|
+-- No
    +-- Re-check I/O-bound or coordination problems.
```

Use:

- Fixed-size executor near `Runtime.getRuntime().availableProcessors()` for predictable CPU work.
- `ForkJoinPool` for recursive divide-and-conquer algorithms.
- Parallel streams only for simple, stateless transformations over sufficiently large data.

Rules of thumb:

- Do not use virtual threads to make CPU-heavy code faster.
- Measure before and after. Parallel code can be slower because of scheduling, memory pressure, and contention.
- Keep shared writes out of the hot loop. Reduce locally, then merge.

### 3. Shared Mutable State

```text
Do multiple threads read/write the same mutable data?
|
+-- No
|   +-- Prefer immutable data, thread confinement, message passing, or local variables.
|
+-- Yes
    |
    +-- Is it a single independent value?
    |   +-- Counter: AtomicInteger, AtomicLong, LongAdder.
    |   +-- Flag: volatile boolean or AtomicBoolean.
    |   +-- Reference swap: AtomicReference.
    |
    +-- Are multiple fields updated under one invariant?
    |   +-- Yes: use synchronized or ReentrantLock.
    |
    +-- Are reads much more common than writes?
    |   +-- Yes: consider immutable snapshots, copy-on-write, ReadWriteLock, or StampedLock.
    |
    +-- Is correctness hard to reason about?
        +-- Prefer a lock or concurrent collection before lock-free code.
```

Use:

- `synchronized` for simple mutual exclusion with clear ownership.
- `ReentrantLock` when you need `tryLock`, interruptible locking, timed locking, or multiple conditions.
- `ConcurrentHashMap`, `BlockingQueue`, `CopyOnWriteArrayList`, and other concurrent collections before building your own.
- `volatile` only for visibility and simple publication, not compound operations.

Rules of thumb:

- `count++` is not atomic unless protected or replaced with an atomic operation.
- Multiple atomics do not automatically make a multi-field invariant atomic.
- Prefer immutability when possible. No shared mutation means fewer concurrency bugs.

### 4. Coordination

```text
Do threads need to wait for a condition, phase, limit, or handoff?
|
+-- One thread waits until another says "ready"
|   +-- CountDownLatch, CompletableFuture, or condition variable.
|
+-- Many workers must start or finish together
|   +-- CountDownLatch for one-shot waiting.
|   +-- CyclicBarrier or Phaser for repeated phases.
|
+-- Producers hand work to consumers
|   +-- BlockingQueue or executor.
|
+-- Only N tasks may access a resource at once
|   +-- Semaphore or bounded executor.
|
+-- A thread waits while protected state changes
    +-- synchronized + wait/notifyAll, or Lock + Condition.
```

Rules of thumb:

- Prefer high-level synchronizers over raw `wait`/`notify`.
- Use bounded queues for backpressure.
- Always wait in a loop when using condition variables because wakeups can be spurious and state can change before the thread reacquires the lock.

### 5. Request-Scoped Concurrency

```text
Are child tasks part of one parent operation?
|
+-- Yes
|   |
|   +-- Should failure of one task cancel the others?
|   |   +-- Yes: structured concurrency with shutdown-on-failure.
|   |
|   +-- Do you need the first successful result?
|   |   +-- Yes: structured concurrency with shutdown-on-success.
|   |
|   +-- Do all child results need to be combined?
|       +-- Yes: fork subtasks, join, then combine results in parent.
|
+-- No
    +-- Use an executor, scheduler, queue, or service-level background worker.
```

Use structured concurrency when these statements are true:

- Child tasks should not outlive the parent request.
- Cancellation should be propagated consistently.
- Exceptions should be collected or reported at the parent boundary.
- The code should read like normal sequential request handling.

## Combination Patterns

Real systems often need more than one tool. Choose one primary model, then add the smallest supporting mechanism.

| Combined problem | Good combination |
| --- | --- |
| REST endpoint calls 3 downstream APIs and combines result | Structured concurrency + virtual threads + per-call timeout |
| Background image processing | Bounded queue + fixed CPU executor + local reduction |
| WebSocket service with async downstream events | Async/reactive API + concurrent session registry + bounded executor for CPU transforms |
| Payment operation updates account and writes audit record | Transaction boundary + lock around in-memory invariant + idempotent external side effect |
| Cache refreshed periodically while many threads read | Immutable snapshot + atomic reference swap + scheduled executor |
| Batch job downloads files then parses them | Virtual threads for download + fixed CPU pool for parsing |
| Rate-limited API client | Semaphore/rate limiter + virtual threads or async HTTP client |
| Multi-stage pipeline | BlockingQueue between stages + bounded executors per stage |

## Anti-Patterns To Catch Early

- Creating raw `Thread` objects for request work instead of using an executor or structured scope.
- Using `CompletableFuture` without controlling the executor.
- Calling blocking I/O on the common `ForkJoinPool`.
- Using `volatile` for compound operations.
- Holding a lock while calling remote services, doing disk I/O, or running callbacks.
- Nesting locks without a fixed ordering.
- Using parallel streams for code with side effects.
- Ignoring interruption and cancellation.
- Making an unbounded queue the default answer to overload.
- Writing lock-free code without jcstress-style tests.

## Final Checklist

Before choosing the implementation, answer these:

1. Is the work I/O-bound, CPU-bound, coordination-heavy, or shared-state-heavy?
2. Can I remove shared mutable state instead of protecting it?
3. What is the lifecycle of each task, and who cancels it?
4. What resource must be bounded: threads, connections, queue size, CPU, memory, or downstream rate?
5. What happens on timeout, failure, and interruption?
6. What invariant must never be broken?
7. How will I prove the choice improved correctness or performance?

If the answer is still unclear, start with the simplest correct version:

```text
Sequential -> immutable/thread-confined -> executor/virtual threads -> structured concurrency -> locks/synchronizers -> atomics -> low-level JMM tools
```

Move right only when the problem actually requires it.
