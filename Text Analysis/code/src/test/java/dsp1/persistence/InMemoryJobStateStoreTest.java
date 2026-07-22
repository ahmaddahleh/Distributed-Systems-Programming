package dsp1.persistence;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryJobStateStoreTest {

    private final Instant now = Instant.parse("2026-07-21T00:00:00Z");

    @Test
    void jobCreationIsIdempotent() {
        InMemoryJobStateStore store = new InMemoryJobStateStore();
        JobRecord job = newJob("job-1");

        assertTrue(store.createJobIfAbsent(job));
        assertFalse(store.createJobIfAbsent(job));

        assertEquals(JobStatus.RECEIVED, store.loadJob("job-1").orElseThrow().status());
    }

    @Test
    void subtaskPersistenceAndParsingCompleteAreIdempotent() {
        InMemoryJobStateStore store = seededStore("job-1");
        List<SubtaskRecord> subtasks = List.of(subtask("job-1", "job-1:0"));

        store.saveSubtasksIfAbsent("job-1", subtasks);
        store.saveSubtasksIfAbsent("job-1", subtasks);
        store.markInputParsingComplete("job-1", 1, now);

        Optional<JobRecord> job = store.loadJob("job-1");
        assertTrue(job.orElseThrow().inputParsingComplete());
        assertEquals(1, job.orElseThrow().expectedSubtaskCount());
        assertEquals(1, store.listSubtasks("job-1").size());
    }

    @Test
    void terminalResultIncrementsCompletedCountOnlyOnce() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        WorkerTerminalResult success = success("job-1", "job-1:0", "result-key");

        assertEquals(TerminalResultStatus.ACCEPTED, store.acceptTerminalResult(success, now));
        assertEquals(TerminalResultStatus.DUPLICATE_OR_CONFLICT, store.acceptTerminalResult(success, now));

        assertEquals(1, store.loadJob("job-1").orElseThrow().completedSubtaskCount());
        assertEquals(SubtaskStatus.SUCCEEDED, store.listSubtasks("job-1").get(0).status());
    }

    @Test
    void firstAcceptedTerminalResultWinsConflict() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();

        assertEquals(TerminalResultStatus.ACCEPTED,
                store.acceptTerminalResult(failure("job-1", "job-1:0", "failed"), now));
        assertEquals(TerminalResultStatus.DUPLICATE_OR_CONFLICT,
                store.acceptTerminalResult(success("job-1", "job-1:0", "late-result"), now));

        SubtaskRecord subtask = store.listSubtasks("job-1").get(0);
        assertEquals(SubtaskStatus.FAILED, subtask.status());
        assertEquals("failed", subtask.errorMessage());
        assertEquals(1, store.loadJob("job-1").orElseThrow().completedSubtaskCount());
    }

    @Test
    void concurrentManagersCanAcceptTerminalResultOnlyOnce() throws Exception {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);

        for (int i = 0; i < 16; i++) {
            executor.submit(() -> {
                try {
                    start.await();
                    if (store.acceptTerminalResult(success("job-1", "job-1:0", "result"), now)
                            == TerminalResultStatus.ACCEPTED) {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, accepted.get());
        assertEquals(1, store.loadJob("job-1").orElseThrow().completedSubtaskCount());
    }

    @Test
    void concurrentWorkersCanClaimSubtaskOnlyOnce() throws Exception {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger claimed = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);

        for (int i = 0; i < 16; i++) {
            String workerId = "worker-" + i;
            executor.submit(() -> {
                try {
                    start.await();
                    if (store.claimSubtaskForProcessing("job-1", "job-1:0", workerId, now, Duration.ofMinutes(2))
                            == SubtaskClaimStatus.CLAIMED) {
                        claimed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, claimed.get());
        assertEquals(SubtaskStatus.PROCESSING, store.listSubtasks("job-1").get(0).status());
    }

    @Test
    void expiredProcessingLeaseCanBeClaimedByAnotherWorker() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();

        assertEquals(SubtaskClaimStatus.CLAIMED,
                store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-a", now, Duration.ofSeconds(30)));
        assertEquals(SubtaskClaimStatus.OWNED_BY_ANOTHER_WORKER,
                store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-b", now.plusSeconds(10), Duration.ofSeconds(30)));
        assertEquals(SubtaskClaimStatus.CLAIMED,
                store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-b", now.plusSeconds(31), Duration.ofSeconds(30)));

        SubtaskRecord subtask = store.listSubtasks("job-1").get(0);
        assertEquals("worker-b", subtask.processingOwner());
        assertEquals(2, subtask.attemptCount());
    }

    @Test
    void activeOwnerCanRenewButNonOwnerCannot() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-a", now, Duration.ofSeconds(30));

        assertEquals(ProcessingLeaseRenewalStatus.RENEWED,
                store.renewProcessingLease("job-1", "job-1:0", "worker-a", now.plusSeconds(10), Duration.ofSeconds(30)));
        assertEquals(ProcessingLeaseRenewalStatus.LOST_OWNERSHIP,
                store.renewProcessingLease("job-1", "job-1:0", "worker-b", now.plusSeconds(11), Duration.ofSeconds(30)));
    }

    @Test
    void terminalSubtaskCannotBeClaimed() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.acceptTerminalResult(success("job-1", "job-1:0", "result"), now);

        assertEquals(SubtaskClaimStatus.ALREADY_TERMINAL,
                store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-a", now, Duration.ofMinutes(2)));
    }

    @Test
    void onlyCurrentProcessingOwnerCanCompleteSubtask() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-a", now, Duration.ofSeconds(30));
        store.claimSubtaskForProcessing("job-1", "job-1:0", "worker-b", now.plusSeconds(31), Duration.ofSeconds(30));

        assertEquals(ClaimedSubtaskCompletionStatus.STALE_OWNER,
                store.completeClaimedSubtask(success("job-1", "job-1:0", "late"), "worker-a", now.plusSeconds(32)));
        assertEquals(ClaimedSubtaskCompletionStatus.COMPLETED,
                store.completeClaimedSubtask(success("job-1", "job-1:0", "winner"), "worker-b", now.plusSeconds(33)));
        assertEquals(1, store.loadJob("job-1").orElseThrow().completedSubtaskCount());
        assertEquals("winner", store.listSubtasks("job-1").get(0).resultS3Key());
    }

    @Test
    void finalizationLeaseAllowsOneOwnerAndExpiredTakeover() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.acceptTerminalResult(success("job-1", "job-1:0", "result"), now);

        assertTrue(store.claimFinalization("job-1", "manager-a", now, Duration.ofMinutes(5)));
        assertFalse(store.claimFinalization("job-1", "manager-b", now.plusSeconds(30), Duration.ofMinutes(5)));
        assertTrue(store.claimFinalization("job-1", "manager-b", now.plus(Duration.ofMinutes(10)), Duration.ofMinutes(5)));
    }

    @Test
    void onlyLeaseOwnerCanCompleteJob() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.acceptTerminalResult(success("job-1", "job-1:0", "result"), now);
        store.claimFinalization("job-1", "manager-a", now, Duration.ofMinutes(5));

        assertFalse(store.markJobCompleted("job-1", "manager-b", "reports/job-1/summary.html", now));
        assertTrue(store.markJobCompleted("job-1", "manager-a", "reports/job-1/summary.html", now));
        assertEquals(JobStatus.COMPLETED, store.loadJob("job-1").orElseThrow().status());
    }

    @Test
    void completedJobNotificationCanBeMarkedSent() {
        InMemoryJobStateStore store = runningStoreWithOneSubtask();
        store.acceptTerminalResult(success("job-1", "job-1:0", "result"), now);
        store.claimFinalization("job-1", "manager-a", now, Duration.ofMinutes(5));
        store.markJobCompleted("job-1", "manager-a", "reports/job-1/summary.html", now);

        assertTrue(store.markCompletionNotificationSent("job-1", now));
        assertEquals(NotificationStatus.SENT, store.loadJob("job-1").orElseThrow().notificationStatus());
    }

    private InMemoryJobStateStore seededStore(String taskId) {
        InMemoryJobStateStore store = new InMemoryJobStateStore();
        store.createJobIfAbsent(newJob(taskId));
        return store;
    }

    private InMemoryJobStateStore runningStoreWithOneSubtask() {
        InMemoryJobStateStore store = seededStore("job-1");
        store.saveSubtasksIfAbsent("job-1", List.of(subtask("job-1", "job-1:0")));
        store.markInputParsingComplete("job-1", 1, now);
        return store;
    }

    private JobRecord newJob(String taskId) {
        return JobRecord.builder(taskId)
                .inputBucket("bucket")
                .inputKey("input")
                .outputBucket("bucket")
                .outputFileName("output.html")
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private SubtaskRecord subtask(String taskId, String subTaskId) {
        return SubtaskRecord.builder(taskId, subTaskId)
                .analysis("POS")
                .url("https://example.com/a.txt")
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private WorkerTerminalResult success(String taskId, String subTaskId, String resultKey) {
        return WorkerTerminalResult.builder(taskId, subTaskId)
                .analysis("POS")
                .url("https://example.com/a.txt")
                .success(true)
                .resultS3Key(resultKey)
                .build();
    }

    private WorkerTerminalResult failure(String taskId, String subTaskId, String error) {
        return WorkerTerminalResult.builder(taskId, subTaskId)
                .analysis("POS")
                .url("https://example.com/a.txt")
                .success(false)
                .errorMessage(error)
                .build();
    }
}
