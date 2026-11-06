package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FanOutWorkerTest {

    private static final String REGION = "EU";
    private static final TemplatePublished EVENT = new TemplatePublished(
            "evt-1", "tpl-1", "v7", REGION, Instant.parse("2026-03-12T10:00:00Z"));

    @Test
    void processesEachEngagementOnlyOnceAcrossRedeliveries() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        Fixture fixture = fixture(List.of(List.of(ref("e1"), ref("e2"), ref("e3"))), config(), gateway);

        fixture.worker().process(EVENT);
        fixture.worker().process(EVENT);

        assertEquals(3, gateway.calls());
        assertEquals(3, fixture.metrics().count("fanout.tasks.skipped_duplicate"));
        assertTrue(fixture.dlq().tasks().isEmpty());
    }

    @Test
    void retriesTransientFailuresAndEventuallySucceeds() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failTimes("e1", 2, () -> new TransientDownstreamException("timeout"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(3, gateway.calls());
        assertEquals(1, fixture.metrics().count("fanout.tasks.succeeded"));
        assertEquals(2, fixture.metrics().count("fanout.tasks.failed_transient"));
        assertTrue(fixture.dlq().tasks().isEmpty());
    }

    @Test
    void sendsPermanentFailureToDlqAndContinuesWithPage() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("e1", new PermanentDownstreamException("400"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"), ref("e2"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(2, gateway.calls());
        assertEquals(1, fixture.dlq().tasks().size());
        assertEquals("e1", fixture.dlq().tasks().get(0).engagementId());
        assertEquals(1, fixture.metrics().count("fanout.tasks.succeeded"));
    }

    @Test
    void sendsUnexpectedRuntimeFailureToDlq() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("e1", new IllegalStateException("boom"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(1, fixture.dlq().tasks().size());
        assertEquals(1, fixture.metrics().count("fanout.tasks.failed_unexpected"));
    }

    @Test
    void respectsMaximumInFlightCalls() throws Exception {
        int maxInFlight = 4;
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(maxInFlight);
        gateway.blockUntil(block, entered);

        List<EngagementRef> refs = IntStream.range(0, 20).mapToObj(i -> ref("e" + i)).toList();
        Fixture fixture = fixture(List.of(refs), config(maxInFlight), gateway);

        Thread runner = new Thread(() -> fixture.worker().process(EVENT));
        runner.start();

        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertEquals(maxInFlight, gateway.maxInFlight());
        block.countDown();
        runner.join(5_000);

        assertFalse(runner.isAlive());
        assertEquals(20, gateway.calls());
        assertTrue(gateway.maxInFlight() <= maxInFlight);
    }

    @Test
    void resumesFromCheckpointWithoutReprocessingEarlierPages() {
        List<List<EngagementRef>> pages = List.of(
                List.of(ref("a1"), ref("a2")),
                List.of(ref("b1"), ref("b2")),
                List.of(ref("c1")));
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        Fixture fixture = fixture(pages, config(), gateway);
        fixture.checkpoints().save(EVENT.eventId(), REGION, "1");

        fixture.worker().process(EVENT);

        assertEquals(List.of("1", "2"), fixture.catalog().cursors());
        assertEquals(3, gateway.calls());
        assertTrue(gateway.callList().stream()
                .map(EngagementRef::engagementId)
                .noneMatch(id -> id.startsWith("a")));
        assertEquals(Optional.of("2"), fixture.checkpoints().load(EVENT.eventId(), REGION));
    }

    @Test
    void stopsReadingPagesAndDrainsInFlightOnShutdown() throws Exception {
        int maxInFlight = 4;
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(maxInFlight);
        gateway.blockUntil(block, entered);

        List<EngagementRef> refs = IntStream.range(0, 20).mapToObj(i -> ref("e" + i)).toList();
        Fixture fixture = fixture(List.of(refs), config(maxInFlight), gateway);

        Thread runner = new Thread(() -> fixture.worker().process(EVENT));
        runner.start();

        assertTrue(entered.await(2, TimeUnit.SECONDS));
        fixture.worker().requestShutdown();
        block.countDown();
        runner.join(5_000);

        assertFalse(runner.isAlive());
        assertEquals(maxInFlight, gateway.calls());
        assertEquals(maxInFlight, gateway.completed());
        assertTrue(gateway.allCompleted());
        assertTrue(fixture.checkpoints().load(EVENT.eventId(), REGION).isEmpty());
        assertEquals(1, fixture.catalog().calls());
    }

    @Test
    void walksAllPagesUsingCursor() {
        List<List<EngagementRef>> pages = List.of(
                List.of(ref("a1")),
                List.of(ref("b1")),
                List.of(ref("c1")));
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        Fixture fixture = fixture(pages, config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(Arrays.asList(null, "1", "2"), fixture.catalog().cursors());
        assertEquals(3, gateway.calls());
    }

    @Test
    void processesDuplicateEngagementWithinEventOnlyOnce() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        Fixture fixture = fixture(List.of(List.of(ref("e1"), ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(1, gateway.calls());
        assertEquals(1, fixture.metrics().count("fanout.tasks.skipped_duplicate"));
    }

    @Test
    void consultsRateLimiterForEveryDownstreamAttempt() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failTimes("e1", 1, () -> new TransientDownstreamException("timeout"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(2, gateway.calls());
        assertEquals(2, fixture.rateLimiter().acquisitions());
    }

    @Test
    void passesIdempotencyKeyAndTimeoutToDownstream() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals("evt-1:e1", gateway.lastKey());
        assertEquals(Duration.ofSeconds(90), gateway.lastTimeout());
    }

    @Test
    void sendsTransientExhaustionToDlq() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failTimes("e1", 3, () -> new TransientDownstreamException("timeout"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);

        fixture.worker().process(EVENT);

        assertEquals(3, gateway.calls());
        assertEquals(1, fixture.dlq().tasks().size());
        assertEquals(3, fixture.metrics().count("fanout.tasks.failed_transient"));
        assertEquals(1, fixture.metrics().count("fanout.tasks.dead_lettered"));
    }

    @Test
    void dlqReplayReleasesClaimAndAllowsReprocessing() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("e1", new PermanentDownstreamException("400"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"), ref("e2"))), config(), gateway);

        fixture.worker().process(EVENT);
        assertEquals(1, fixture.dlq().tasks().size());

        new DlqReplay(fixture.idempotency()).release(fixture.dlq().tasks().get(0));
        fixture.worker().process(EVENT);

        assertEquals(3, gateway.calls());
        assertEquals(2, fixture.metrics().count("fanout.tasks.succeeded"));
        assertEquals(1, fixture.dlq().tasks().size());
    }

    @Test
    void dlqReplayReprocessesFailedEngagementWithoutRewindingCursor() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("a1", new PermanentDownstreamException("400"));
        Fixture fixture = fixture(List.of(
                List.of(ref("a1"), ref("a2")),
                List.of(ref("b1"))), config(), gateway);

        fixture.worker().process(EVENT);
        assertEquals(1, fixture.dlq().tasks().size());
        assertEquals(Optional.of("1"), fixture.checkpoints().load(EVENT.eventId(), REGION));

        assertTrue(new DlqReplay(fixture.idempotency())
                .replay(EVENT, fixture.dlq().tasks().get(0), fixture.worker()));

        assertEquals(4, gateway.calls());
        assertEquals(1, fixture.dlq().tasks().size());
        assertEquals(Optional.of("1"), fixture.checkpoints().load(EVENT.eventId(), REGION));
    }

    @Test
    void replayReturnsFalseWhenWorkerIsShutdown() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("e1", new PermanentDownstreamException("400"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);
        fixture.worker().process(EVENT);

        FailedTask task = fixture.dlq().tasks().get(0);
        fixture.worker().requestShutdown();

        assertFalse(new DlqReplay(fixture.idempotency()).replay(EVENT, task, fixture.worker()));
    }

    @Test
    void enforcesPageDeadline() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        CountDownLatch never = new CountDownLatch(1);
        gateway.blockUntil(never, new CountDownLatch(1));
        FanOutWorkerConfig shortConfig = new FanOutWorkerConfig(
                100,
                1,
                1,
                Duration.ZERO,
                Duration.ofMillis(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), shortConfig, gateway);

        assertThrows(IllegalStateException.class, () -> fixture.worker().process(EVENT));
    }

    @Test
    void dlqReplayRejectsMismatchedEvent() {
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        gateway.failNext("e1", new PermanentDownstreamException("400"));
        Fixture fixture = fixture(List.of(List.of(ref("e1"))), config(), gateway);
        fixture.worker().process(EVENT);

        FailedTask task = fixture.dlq().tasks().get(0);
        TemplatePublished other = new TemplatePublished(
                "evt-2", "tpl-1", "v7", REGION, Instant.parse("2026-03-13T10:00:00Z"));

        assertThrows(IllegalArgumentException.class,
                () -> new DlqReplay(fixture.idempotency()).replay(other, task, fixture.worker()));
    }

    @Test
    void serializesConcurrentDeliveriesOfSameEvent() throws Exception {
        int maxInFlight = 2;
        ScriptedPendingUpdateGateway gateway = new ScriptedPendingUpdateGateway();
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        gateway.blockUntil(block, entered);
        Fixture fixture = fixture(List.of(List.of(ref("e1"), ref("e2"))), config(maxInFlight), gateway);

        Thread first = new Thread(() -> fixture.worker().process(EVENT));
        first.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        Thread second = new Thread(() -> fixture.worker().process(EVENT));
        second.start();
        Thread.sleep(100);
        block.countDown();

        first.join(5_000);
        second.join(5_000);

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertEquals(2, gateway.calls());
        assertTrue(gateway.maxInFlight() <= maxInFlight);
    }

    private static FanOutWorkerConfig config() {
        return config(4);
    }

    private static FanOutWorkerConfig config(int maxInFlight) {
        return new FanOutWorkerConfig(
                100,
                maxInFlight,
                3,
                Duration.ofMillis(10),
                Duration.ofMillis(40),
                Duration.ofSeconds(90),
                Duration.ofSeconds(2));
    }

    private static Fixture fixture(
            List<List<EngagementRef>> pages,
            FanOutWorkerConfig config,
            ScriptedPendingUpdateGateway gateway) {
        InMemoryEngagementCatalog catalog = new InMemoryEngagementCatalog(pages);
        InMemoryIdempotencyStore idempotency = new InMemoryIdempotencyStore();
        CountingRateLimiter rateLimiter = new CountingRateLimiter();
        InMemoryCheckpointStore checkpoints = new InMemoryCheckpointStore();
        RecordingDeadLetterSink dlq = new RecordingDeadLetterSink();
        RecordingMetrics metrics = new RecordingMetrics();
        FanOutWorker worker = new FanOutWorker(
                catalog,
                gateway,
                idempotency,
                rateLimiter,
                checkpoints,
                dlq,
                metrics,
                config,
                new FakeSleeper(new ManualNanoClock(0)),
                new Random(42));
        return new Fixture(catalog, idempotency, rateLimiter, checkpoints, dlq, metrics, worker);
    }

    private static EngagementRef ref(String engagementId) {
        return new EngagementRef("firm-1", engagementId, REGION);
    }

    private record Fixture(
            InMemoryEngagementCatalog catalog,
            InMemoryIdempotencyStore idempotency,
            CountingRateLimiter rateLimiter,
            InMemoryCheckpointStore checkpoints,
            RecordingDeadLetterSink dlq,
            RecordingMetrics metrics,
            FanOutWorker worker) {
    }
}
