package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.random.RandomGenerator;

public final class FanOutWorker {

    private final EngagementCatalog catalog;
    private final PendingUpdateGateway gateway;
    private final IdempotencyStore idempotencyStore;
    private final RateLimiter rateLimiter;
    private final ConcurrencyLimiter concurrencyLimiter;
    private final CheckpointStore checkpointStore;
    private final DeadLetterSink deadLetterSink;
    private final Metrics metrics;
    private final FanOutWorkerConfig config;
    private final Sleeper sleeper;
    private final RandomGenerator random;
    private static final int LOCK_STRIPES = 256;
    private static final Duration SHUTDOWN_POLL = Duration.ofMillis(100);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final ReentrantLock[] eventLocks = createLocks();

    public FanOutWorker(
            EngagementCatalog catalog,
            PendingUpdateGateway gateway,
            IdempotencyStore idempotencyStore,
            RateLimiter rateLimiter,
            CheckpointStore checkpointStore,
            DeadLetterSink deadLetterSink,
            Metrics metrics,
            FanOutWorkerConfig config) {
        this(catalog, gateway, idempotencyStore, rateLimiter,
                new SemaphoreConcurrencyLimiter(config.maxInFlight()),
                checkpointStore, deadLetterSink, metrics, config, Sleeper.system(), new Random());
    }

    public FanOutWorker(
            EngagementCatalog catalog,
            PendingUpdateGateway gateway,
            IdempotencyStore idempotencyStore,
            RateLimiter rateLimiter,
            CheckpointStore checkpointStore,
            DeadLetterSink deadLetterSink,
            Metrics metrics,
            FanOutWorkerConfig config,
            Sleeper sleeper,
            RandomGenerator random) {
        this(catalog, gateway, idempotencyStore, rateLimiter,
                new SemaphoreConcurrencyLimiter(config.maxInFlight()),
                checkpointStore, deadLetterSink, metrics, config, sleeper, random);
    }

    public FanOutWorker(
            EngagementCatalog catalog,
            PendingUpdateGateway gateway,
            IdempotencyStore idempotencyStore,
            RateLimiter rateLimiter,
            ConcurrencyLimiter concurrencyLimiter,
            CheckpointStore checkpointStore,
            DeadLetterSink deadLetterSink,
            Metrics metrics,
            FanOutWorkerConfig config,
            Sleeper sleeper,
            RandomGenerator random) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.idempotencyStore = Objects.requireNonNull(idempotencyStore, "idempotencyStore");
        this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter");
        this.concurrencyLimiter = Objects.requireNonNull(concurrencyLimiter, "concurrencyLimiter");
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
        this.deadLetterSink = Objects.requireNonNull(deadLetterSink, "deadLetterSink");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.config = Objects.requireNonNull(config, "config");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.random = Objects.requireNonNull(random, "random");
    }

    public void requestShutdown() {
        shutdownRequested.set(true);
    }

    public boolean isShutdownRequested() {
        return shutdownRequested.get();
    }

    public void process(TemplatePublished event) {
        Objects.requireNonNull(event, "event");
        if (shutdownRequested.get()) {
            return;
        }

        ReentrantLock lock = lockFor(event);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.increment("fanout.worker.interrupted", tags(event));
            return;
        }

        try {
            processLocked(event);
        } finally {
            lock.unlock();
        }
    }

    public boolean reprocess(TemplatePublished event, EngagementRef ref) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(ref, "ref");
        if (shutdownRequested.get()) {
            return false;
        }

        ReentrantLock lock = lockFor(event);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.increment("fanout.worker.interrupted", tags(event));
            return false;
        }

        try {
            boolean acquired = false;
            while (!acquired) {
                try {
                    acquired = concurrencyLimiter.tryAcquire(SHUTDOWN_POLL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    metrics.increment("fanout.worker.interrupted", tags(event));
                    return false;
                }
                if (!acquired && shutdownRequested.get()) {
                    return false;
                }
            }
            try {
                processEngagement(event, ref, tags(event));
                return true;
            } finally {
                concurrencyLimiter.release();
            }
        } finally {
            lock.unlock();
        }
    }

    private void processLocked(TemplatePublished event) {
        if (shutdownRequested.get()) {
            return;
        }

        Map<String, String> tags = tags(event);
        String cursor = checkpointStore.load(event.eventId(), event.region()).orElse(null);
        ExecutorService executor = Executors.newFixedThreadPool(config.maxInFlight(), threadFactory());

        try {
            while (!shutdownRequested.get()) {
                Page page = catalog.findEngagementsUsingTemplate(
                        event.templateId(), event.region(), cursor, config.pageSize());
                metrics.increment("fanout.pages.read", tags);

                List<Future<?>> futures = new ArrayList<>(page.items().size());
                boolean pageComplete = true;

                long pageDeadline = System.nanoTime() + taskWaitBudget().toNanos();
                for (EngagementRef ref : page.items()) {
                    boolean acquired = false;
                    while (!acquired) {
                        acquired = concurrencyLimiter.tryAcquire(SHUTDOWN_POLL);
                        if (!acquired && shutdownRequested.get()) {
                            pageComplete = false;
                            break;
                        }
                    }
                    if (!acquired) {
                        break;
                    }
                    if (shutdownRequested.get()) {
                        concurrencyLimiter.release();
                        pageComplete = false;
                        break;
                    }
                    try {
                        futures.add(executor.submit(() -> {
                            try {
                                processEngagement(event, ref, tags);
                            } finally {
                                concurrencyLimiter.release();
                            }
                        }));
                    } catch (RuntimeException e) {
                        concurrencyLimiter.release();
                        throw e;
                    }
                }

                awaitAll(futures, pageDeadline);

                if (!pageComplete || shutdownRequested.get()) {
                    break;
                }
                if (page.nextCursor() == null) {
                    break;
                }
                checkpointStore.save(event.eventId(), event.region(), page.nextCursor());
                cursor = page.nextCursor();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.increment("fanout.worker.interrupted", tags);
        } finally {
            shutdownExecutor(executor, tags);
        }
    }

    private void awaitAll(List<Future<?>> futures, long deadlineNanos) throws InterruptedException {
        for (Future<?> future : futures) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                future.cancel(true);
                metrics.increment("fanout.worker.task_timeout", Map.of());
                throw new IllegalStateException("fan-out page exceeded its lifetime budget");
            }
            try {
                future.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                metrics.increment("fanout.worker.task_timeout", Map.of());
                throw new IllegalStateException("fan-out page exceeded its lifetime budget", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("fan-out task failed", e.getCause());
            }
        }
    }

    private Duration taskWaitBudget() {
        long perAttempt = config.downstreamTimeout().toMillis() + config.maxBackoff().toMillis();
        return Duration.ofMillis(Math.max(1, config.maxAttempts() * perAttempt + 1_000L));
    }

    private void processEngagement(TemplatePublished event, EngagementRef ref, Map<String, String> tags) {
        String key = event.eventId() + ":" + ref.engagementId();
        if (!idempotencyStore.claim(key)) {
            metrics.increment("fanout.tasks.skipped_duplicate", tags);
            return;
        }

        try {
            int attempt = 0;
            while (true) {
                if (shutdownRequested.get()) {
                    idempotencyStore.release(key);
                    metrics.increment("fanout.tasks.released", tags);
                    return;
                }

                attempt++;
                metrics.increment("fanout.downstream.attempts", tags);
                try {
                    rateLimiter.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    idempotencyStore.release(key);
                    metrics.increment("fanout.tasks.released", tags);
                    return;
                }

                long startedAt = System.nanoTime();
                try {
                    gateway.markPending(ref, event.templateId(), event.version(), key,
                            config.downstreamTimeout());
                    metrics.timing("fanout.downstream.latency",
                            Duration.ofNanos(System.nanoTime() - startedAt), tags);
                    metrics.increment("fanout.tasks.succeeded", tags);
                    return;
                } catch (TransientDownstreamException e) {
                    metrics.increment("fanout.tasks.failed_transient", tags);
                    if (attempt >= config.maxAttempts()) {
                        deadLetterSink.send(failedTask(event, ref, attempt), e);
                        metrics.increment("fanout.tasks.dead_lettered", tags);
                        return;
                    }
                    if (shutdownRequested.get()) {
                        idempotencyStore.release(key);
                        metrics.increment("fanout.tasks.released", tags);
                        return;
                    }
                    try {
                        sleeper.sleep(backoff(attempt));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        idempotencyStore.release(key);
                        metrics.increment("fanout.tasks.released", tags);
                        return;
                    }
                } catch (PermanentDownstreamException e) {
                    metrics.increment("fanout.tasks.failed_permanent", tags);
                    deadLetterSink.send(failedTask(event, ref, attempt), e);
                    metrics.increment("fanout.tasks.dead_lettered", tags);
                    return;
                } catch (RuntimeException e) {
                    metrics.increment("fanout.tasks.failed_unexpected", tags);
                    deadLetterSink.send(failedTask(event, ref, attempt), e);
                    metrics.increment("fanout.tasks.dead_lettered", tags);
                    return;
                }
            }
        } catch (RuntimeException e) {
            idempotencyStore.release(key);
            throw e;
        }
    }

    private FailedTask failedTask(TemplatePublished event, EngagementRef ref, int attempts) {
        return new FailedTask(
                event.eventId(),
                ref.firmId(),
                ref.engagementId(),
                event.templateId(),
                event.version(),
                event.region(),
                attempts);
    }

    private Duration backoff(int attempt) {
        long base = Math.max(0, config.initialBackoff().toMillis());
        long maximum = Math.max(base, config.maxBackoff().toMillis());
        int shift = Math.min(attempt - 1, 20);
        long exponential = base == 0 ? 0 : Math.min(maximum, base * (1L << shift));
        long half = Math.max(1, exponential / 2);
        long jitter = random.nextLong(half + 1);
        return Duration.ofMillis(half + jitter);
    }

    private void shutdownExecutor(ExecutorService executor, Map<String, String> tags) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                metrics.increment("fanout.worker.forced_shutdown", tags);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ReentrantLock lockFor(TemplatePublished event) {
        int index = Math.floorMod((event.eventId() + ":" + event.region()).hashCode(), LOCK_STRIPES);
        return eventLocks[index];
    }

    private static ReentrantLock[] createLocks() {
        ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
        return locks;
    }

    private static Map<String, String> tags(TemplatePublished event) {
        return Map.of(
                "templateId", event.templateId(),
                "version", event.version(),
                "region", event.region());
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "fanout-worker-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
