# Part 2: template publish fan-out worker

Processes a `TemplatePublished` event, pages the engagement catalog for the affected template and region, and marks each engagement as pending through a downstream gateway that takes about one minute per engagement and has limited capacity.

This is the slow path. It is not the source of the seconds-level pending indicator described in the design document.

Java 21 and Maven.

## Assumptions

- The catalog is paginated by cursor and does not load engagements.
- The downstream takes about one minute per engagement and has a known quota.
- Event delivery is at least once.
- The downstream accepts an idempotency key and a timeout, or exposes a status check. Without that, a lost response after a successful call is ambiguous.
- One worker processes one event per `process` call. Scaling is horizontal, per event and region.

## Concurrency

- Fixed pool of `maxInFlight` threads. Never one thread per engagement.
- A semaphore with `maxInFlight` permits sits between the page reader and the workers. When permits are exhausted, the reader blocks, so it stops reading pages instead of buffering unbounded work. That is the backpressure.
- A token bucket gates every downstream attempt. Set permits per second below the agreed quota. The worker also caps calls in flight, so capacity is enforced twice: by rate and by concurrency.
- The reader waits for the page to finish before saving the checkpoint. That bounds reprocessing to one page.
- New publications should take priority over backfill. That is an operational concern outside this component: separate queues or deployments, or a priority field on the event.
- The token bucket is per worker instance. Production must share it across replicas or partition workers so the sum of quotas equals the downstream quota. The `ConcurrencyLimiter` is injectable and can be shared the same way.
- A fixed set of 256 striped per-event locks serializes event processing for checkpoint consistency inside one worker with bounded memory. The idempotency claim is the primary defense against duplicate downstream calls. Across replicas, use a message group or a distributed lock.

## Failures and idempotency

- Claim key: `eventId:engagementId`. Claim before the downstream call.
- `IdempotencyStore.release` returns the claim on shutdown, interruption, or an unexpected local failure. Without it, a claimed but unfinished engagement would be skipped forever.
- Transient failures: retry with exponential backoff and jitter up to `maxAttempts`, then DLQ.
- Permanent failures: DLQ immediately and keep processing the page.
- Unexpected `RuntimeException`: DLQ. Retrying a bug does not help. The gateway must translate transport failures into `TransientDownstreamException`; wrapping them in `RuntimeException` would classify them as permanent.
- Duplicate event: the claim skips engagements already processed.
- Hard crash caveat: a crash after the claim and before completion leaves the key claimed. In production the store needs a TTL longer than event retention, or a two-phase claim (`in_progress` then `completed`). `release` only covers orderly paths.
- Downstream timeout: the gateway must honor the timeout (about 90 seconds) and return a transient failure. A lost response after a successful call is ambiguous, which is why the idempotency key or a status check is required.
- DLQ replay: `DlqReplay` releases the claim and can re-submit the failed engagement directly through `worker.reprocess`, so the replay does not depend on the page cursor. It returns true only when the worker actually reprocessed. The adapter must ack the DLQ message only on true; on false, leave it for redelivery.

## Checkpointing and resume

- One cursor per `(eventId, region)`, saved per page.
- The checkpoint is saved only after every task in the page finishes.
- After a crash, at most one page is reprocessed, and idempotency prevents duplicate effects.
- The last page is not checkpointed when `nextCursor` is null, so it is reprocessed once on resume. That is intentional and bounded.

## Graceful shutdown

- `requestShutdown()` stops reading new pages and submitting new tasks.
- In-flight tasks are drained under one page deadline (attempts times downstream timeout plus backoff), shared by all futures in the page. The reader polls the concurrency limiter so a shutdown request is observed even when it is waiting for a permit.
- A partially processed page is not checkpointed, so it is retried.
- If the executor does not terminate within `shutdownTimeout`, it is forced down and a metric is emitted.

## What was simplified

- In-memory idempotency and checkpoint stores.
- One executor per `process` call.
- No catalog retry, no distributed lock, no priority queue, and no batch downstream API.
- Metrics are an interface with no real backend.
- No seed control for jitter in production.
- The token bucket is in memory and per worker, so the quota is not shared across instances.
- There is no built-in replay queue. `DlqReplay` releases the claim and reprocesses one engagement directly; the queue, scheduler and ack are external.

## What I would change for production

- SQS or Kafka per region, with DLQ and replay tooling.
- Distributed idempotency with TTL or a two-phase claim, for example DynamoDB conditional writes.
- A shared token bucket across replicas, or partitioned workers that split the quota.
- A shared worker pool or virtual threads, sized to the downstream quota.
- Priority for new publications over backfill.
- Dynamic quota from configuration or from the downstream team.
- OpenTelemetry traces and per-tenant metrics.
- Poison event quarantine and alerting.
- Load tests at the agreed quota.

## Tradeoffs

| Decision | Alternative | Why |
|---|---|---|
| At least once with idempotency | Exactly once | Simpler and safe; duplicates are cheap, lost work is not |
| Checkpoint per page | Checkpoint per engagement | Fewer writes; at most one page reprocessed |
| Fixed pool | Virtual threads or thread per engagement | Makes the concurrency limit explicit |
| Claim before the call, release on orderly failure | Claim after success | Avoids duplicate downstream calls; requires release or TTL |
| Retry then DLQ | Infinite retry | A poison event must not block the pipeline |
| One message per event and region, internal paging | One message per engagement | Fewer queue messages; page checkpoint fits naturally |
| Semaphore backpressure | Unbounded queue | Prevents memory growth and downstream overload |
| Idempotency key and timeout in the downstream contract | Infer only from the worker | Prevents duplicate effects after a lost response |
| Shared quota across replicas | Per-worker token bucket | Prevents aggregate overuse |

## Run

```bash
mvn test
mvn package
```

## Wiring

```java
double permitsPerSecond = 10.0;
double burst = 20.0;

FanOutWorker worker = new FanOutWorker(
        catalog,
        gateway,
        idempotencyStore,
        new TokenBucketRateLimiter(permitsPerSecond, burst),
        checkpointStore,
        deadLetterSink,
        metrics,
        FanOutWorkerConfig.defaults());

worker.process(event);
```

Set `permitsPerSecond` and `burst` at or below the quota agreed with the downstream team. Set `maxInFlight` to the number of calls the downstream can absorb at once, not higher. The default is 56 concurrent calls, which meets the 6-hour target for 20,000 engagements; lower it to the agreed quota. The gateway implementation must honor the idempotency key and the timeout passed by the worker.

## Test coverage

- Idempotency across redeliveries.
- Duplicate engagement within one event.
- Transient retry with backoff and eventual success.
- Permanent failure to DLQ without stopping the page.
- Unexpected runtime failure to DLQ.
- Maximum in-flight concurrency.
- Checkpoint resume without reprocessing earlier pages.
- Graceful shutdown drains in-flight tasks and does not checkpoint a partial page.
- Cursor pagination across all pages.
- Rate limiter called once per downstream attempt.
- Token bucket burst, wait, and sustained rate.
- Idempotency key and timeout passed to the downstream.
- Transient exhaustion to DLQ.
- DLQ replay releases the claim and allows reprocessing.
- Replay returns false when the worker is in shutdown.
- The page deadline is enforced across all futures.
- Multi-page DLQ replay reprocesses the failed engagement without rewinding the cursor.
- Concurrent deliveries of the same event are serialized and stay within the in-flight limit.
- DLQ replay rejects an event that does not match the failed task.
