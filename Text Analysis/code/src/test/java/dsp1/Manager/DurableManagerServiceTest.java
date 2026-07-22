package dsp1.Manager;

import dsp1.persistence.InMemoryJobStateStore;
import dsp1.persistence.JobRecord;
import dsp1.persistence.JobStatus;
import dsp1.persistence.NotificationStatus;
import dsp1.persistence.SubtaskRecord;
import dsp1.persistence.SubtaskStatus;
import dsp1.persistence.TerminalResultStatus;
import dsp1.persistence.WorkerTerminalResult;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableManagerServiceTest {
    private static final Instant NOW = Instant.parse("2026-07-21T00:00:00Z");
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void jobRequestPersistenceAllowsAcknowledgement() {
        InMemoryJobStateStore store = new InMemoryJobStateStore();
        DurableManagerService service = service(store, storage("POS\thttps://example.com/a.txt"), new FakeQueues());

        assertTrue(service.persistJobRequest(jobRequest("job-1")));

        assertTrue(store.loadJob("job-1").isPresent());
    }

    @Test
    void persistenceFailurePreventsAcknowledgement() {
        FailingCreateStore store = new FailingCreateStore();
        DurableManagerService service = service(store, storage("POS\thttps://example.com/a.txt"), new FakeQueues());

        assertThrows(RuntimeException.class, () -> service.persistJobRequest(jobRequest("job-1")));
    }

    @Test
    void duplicateJobRequestIsIdempotentAndRetryAfterPersistBeforeAckIsSafe() {
        InMemoryJobStateStore store = new InMemoryJobStateStore();
        DurableManagerService service = service(store, storage("POS\thttps://example.com/a.txt"), new FakeQueues());

        service.persistJobRequest(jobRequest("job-1"));
        service.persistJobRequest(jobRequest("job-1"));
        service.recoverJob("job-1");

        assertEquals(1, store.listSubtasks("job-1").size());
    }

    @Test
    void recoversReceivedJobByParsingInputAndDispatchingPendingTasks() {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        FakeQueues queues = new FakeQueues();
        DurableManagerService service = service(store, storage("""
                POS\thttps://example.com/a.txt
                bad-line
                POS\thttps://example.com/a.txt
                """), queues);

        service.recoverJob("job-1");

        JobRecord job = store.loadJob("job-1").orElseThrow();
        assertTrue(job.inputParsingComplete());
        assertEquals(2, job.expectedSubtaskCount());
        assertEquals(2, queues.workerTasks.size());
        assertEquals("job-1:0", queues.workerTasks.get(0).getString("subTaskId"));
        assertEquals("job-1:1", queues.workerTasks.get(1).getString("subTaskId"));
    }

    @Test
    void crashDuringInputParsingIsIdempotentlyRecovered() {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        List<SubtaskRecord> partial = List.of(SubtaskRecord.builder("job-1", "job-1:0")
                .analysis("POS")
                .url("https://example.com/a.txt")
                .createdAt(NOW)
                .updatedAt(NOW)
                .build());
        store.saveSubtasksIfAbsent("job-1", partial);
        FakeQueues queues = new FakeQueues();
        DurableManagerService service = service(store, storage("""
                POS\thttps://example.com/a.txt
                POS\thttps://example.com/b.txt
                """), queues);

        service.recoverJob("job-1");

        assertEquals(2, store.listSubtasks("job-1").size());
        assertTrue(store.loadJob("job-1").orElseThrow().inputParsingComplete());
    }

    @Test
    void staleDispatchedSubtaskIsRedispatchedButTerminalSubtaskIsNot() {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        store.saveSubtasksIfAbsent("job-1", List.of(
                subtask("job-1", "job-1:0", SubtaskStatus.DISPATCHED, NOW.minusSeconds(600)),
                subtask("job-1", "job-1:1", SubtaskStatus.SUCCEEDED, NOW.minusSeconds(600))));
        store.markInputParsingComplete("job-1", 2, NOW);
        store.acceptTerminalResult(success("job-1", "job-1:1"), NOW);
        FakeQueues queues = new FakeQueues();

        service(store, storage(""), queues).recoverJob("job-1");

        assertEquals(1, queues.workerTasks.size());
        assertEquals("job-1:0", queues.workerTasks.get(0).getString("subTaskId"));
    }

    @Test
    void crashAfterDispatchBeforeMarkingDispatchedCausesSafeRedispatch() {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        store.saveSubtasksIfAbsent("job-1", List.of(subtask("job-1", "job-1:0", SubtaskStatus.PENDING, NOW)));
        store.markInputParsingComplete("job-1", 1, NOW);
        FakeQueues queues = new FakeQueues();
        queues.failAfterWorkerSend = true;

        assertThrows(RuntimeException.class, () -> service(store, storage(""), queues).recoverJob("job-1"));
        queues.failAfterWorkerSend = false;
        service(store, storage(""), queues).recoverJob("job-1");

        assertEquals(2, queues.workerTasks.size());
        assertEquals(SubtaskStatus.DISPATCHED, store.listSubtasks("job-1").get(0).status());
    }

    @Test
    void workerResultAcceptedWhileManagerWasOfflineAndDuplicateDeliveryIsHarmless() {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        DurableManagerService service = service(store, storage(""), new FakeQueues());
        JSONObject result = workerSuccess("job-1", "job-1:0");

        assertEquals(TerminalResultStatus.ACCEPTED, service.handleWorkerResult(result));
        assertEquals(TerminalResultStatus.DUPLICATE_OR_CONFLICT, service.handleWorkerResult(result));

        assertEquals(1, store.loadJob("job-1").orElseThrow().completedSubtaskCount());
    }

    @Test
    void successFailureConflictKeepsFirstTerminalResult() {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        DurableManagerService service = service(store, storage(""), new FakeQueues());

        assertEquals(TerminalResultStatus.ACCEPTED, service.handleWorkerResult(workerFailure("job-1", "job-1:0")));
        assertEquals(TerminalResultStatus.DUPLICATE_OR_CONFLICT, service.handleWorkerResult(workerSuccess("job-1", "job-1:0")));

        assertEquals(SubtaskStatus.FAILED, store.listSubtasks("job-1").get(0).status());
    }

    @Test
    void resultPersistenceFailurePreventsAcknowledgementAtCaller() {
        DurableManagerService service = service(new FailingResultStore(), storage(""), new FakeQueues());

        assertThrows(RuntimeException.class, () -> service.handleWorkerResult(workerSuccess("job-1", "job-1:0")));
    }

    @Test
    void concurrentManagersProcessingSameResultOnlyIncrementOnce() throws Exception {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        DurableManagerService service = service(store, storage(""), new FakeQueues());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        AtomicInteger accepted = new AtomicInteger();

        for (int i = 0; i < 12; i++) {
            executor.submit(() -> {
                try {
                    start.await();
                    if (service.handleWorkerResult(workerSuccess("job-1", "job-1:0")) == TerminalResultStatus.ACCEPTED) {
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
    void allTasksCompleteBeforeReportGenerationIsRecoveredAndFinalized() {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        store.acceptTerminalResult(success("job-1", "job-1:0"), NOW);
        FakeStorage storage = storage("");
        FakeQueues queues = new FakeQueues();

        service(store, storage, queues).recoverJob("job-1");

        JobRecord job = store.loadJob("job-1").orElseThrow();
        assertEquals(JobStatus.COMPLETED, job.status());
        assertEquals("reports/job-1/summary.html", job.finalReportKey());
        assertTrue(storage.objects.containsKey("bucket/reports/job-1/summary.html"));
        assertEquals(NotificationStatus.SENT, job.notificationStatus());
    }

    @Test
    void reportUploadedBeforeCompletedCanBeRegeneratedAfterLeaseExpiry() {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        store.acceptTerminalResult(success("job-1", "job-1:0"), NOW);
        store.claimFinalization("job-1", "old-manager", NOW.minusSeconds(600), Duration.ofSeconds(60));
        FakeStorage storage = storage("");
        FakeQueues queues = new FakeQueues();

        service(store, storage, queues).recoverJob("job-1");

        assertEquals(JobStatus.COMPLETED, store.loadJob("job-1").orElseThrow().status());
        assertTrue(storage.objects.containsKey("bucket/reports/job-1/summary.html"));
    }

    @Test
    void twoManagersCompeteForFinalizationButOnlyOneReportIsConcurrentWinner() throws Exception {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        store.acceptTerminalResult(success("job-1", "job-1:0"), NOW);
        FakeStorage storage = storage("");
        FakeQueues queues = new FakeQueues();
        DurableManagerService first = service(store, storage, queues);
        DurableManagerService second = new DurableManagerService(
                store, storage, queues, clock,
                new DurableManagerConfig("manager-b", Duration.ofMinutes(5), Duration.ofMinutes(5), Duration.ofSeconds(30)));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger finalized = new AtomicInteger();
        executor.submit(() -> runFinalize(first, start, finalized));
        executor.submit(() -> runFinalize(second, start, finalized));
        start.countDown();
        executor.shutdown();

        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, storage.putCount);
        assertEquals(JobStatus.COMPLETED, store.loadJob("job-1").orElseThrow().status());
    }

    @Test
    void notificationFailureLeavesPendingAndRecoveryRetries() {
        InMemoryJobStateStore store = completedJobPendingNotification();
        FakeQueues queues = new FakeQueues();
        queues.failLocalSend = true;
        DurableManagerService service = service(store, storage(""), queues);

        assertThrows(RuntimeException.class, () -> service.recoverJob("job-1"));
        assertEquals(NotificationStatus.PENDING, store.loadJob("job-1").orElseThrow().notificationStatus());

        queues.failLocalSend = false;
        service.recoverJob("job-1");

        assertEquals(NotificationStatus.SENT, store.loadJob("job-1").orElseThrow().notificationStatus());
        assertEquals(2, queues.localCompletions.size());
    }

    @Test
    void crashAfterNotificationSendBeforeStatusUpdateCausesDuplicateCompletionNotification() {
        InMemoryJobStateStore store = completedJobPendingNotification();
        FakeQueues queues = new FakeQueues();
        DurableManagerService service = service(new FailingNotificationMarkStore(store), storage(""), queues);

        assertThrows(RuntimeException.class, () -> service.recoverJob("job-1"));
        assertEquals(1, queues.localCompletions.size());

        service(store, storage(""), queues).recoverJob("job-1");

        assertEquals(2, queues.localCompletions.size());
        assertEquals(NotificationStatus.SENT, store.loadJob("job-1").orElseThrow().notificationStatus());
    }

    @Test
    void zeroValidTaskJobFinalizesSuccessfully() {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        FakeStorage storage = storage("bad-line");
        FakeQueues queues = new FakeQueues();

        service(store, storage, queues).recoverJob("job-1");

        assertEquals(JobStatus.COMPLETED, store.loadJob("job-1").orElseThrow().status());
        assertEquals(0, store.loadJob("job-1").orElseThrow().expectedSubtaskCount());
    }

    private void runFinalize(DurableManagerService service, CountDownLatch start, AtomicInteger finalized) {
        try {
            start.await();
            if (service.tryFinalize("job-1")) {
                finalized.incrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private DurableManagerService service(InMemoryJobStateStore store, FakeStorage storage, FakeQueues queues) {
        return new DurableManagerService(store, storage, queues, clock,
                new DurableManagerConfig("manager-a", Duration.ofMinutes(5), Duration.ofMinutes(5), Duration.ofSeconds(30)));
    }

    private FakeStorage storage(String input) {
        FakeStorage storage = new FakeStorage();
        storage.objects.put("bucket/input", input);
        return storage;
    }

    private JSONObject jobRequest(String taskId) {
        return new JSONObject()
                .put("type", "newTask")
                .put("taskId", taskId)
                .put("s3Bucket", "bucket")
                .put("key", "input")
                .put("outputFile", "output.html")
                .put("terminate", false);
    }

    private InMemoryJobStateStore seededReceivedJob(String taskId) {
        InMemoryJobStateStore store = new InMemoryJobStateStore();
        store.createJobIfAbsent(JobRecord.builder(taskId)
                .inputBucket("bucket")
                .inputKey("input")
                .outputBucket("bucket")
                .outputFileName("output.html")
                .createdAt(NOW)
                .updatedAt(NOW)
                .build());
        return store;
    }

    private InMemoryJobStateStore runningJobWithSubtasks(int count) {
        InMemoryJobStateStore store = seededReceivedJob("job-1");
        List<SubtaskRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(subtask("job-1", "job-1:" + i, SubtaskStatus.PENDING, NOW));
        }
        store.saveSubtasksIfAbsent("job-1", records);
        store.markInputParsingComplete("job-1", count, NOW);
        return store;
    }

    private InMemoryJobStateStore completedJobPendingNotification() {
        InMemoryJobStateStore store = runningJobWithSubtasks(1);
        store.acceptTerminalResult(success("job-1", "job-1:0"), NOW);
        store.claimFinalization("job-1", "manager-a", NOW, Duration.ofMinutes(5));
        store.markJobCompleted("job-1", "manager-a", "reports/job-1/summary.html", NOW);
        return store;
    }

    private SubtaskRecord subtask(String taskId, String subTaskId, SubtaskStatus status, Instant dispatchedAt) {
        return SubtaskRecord.builder(taskId, subTaskId)
                .analysis("POS")
                .url("https://example.com/a.txt")
                .status(status)
                .dispatchedAt(dispatchedAt)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }

    private WorkerTerminalResult success(String taskId, String subTaskId) {
        return WorkerTerminalResult.builder(taskId, subTaskId)
                .analysis("POS")
                .url("https://example.com/a.txt")
                .success(true)
                .resultS3Key("results/" + subTaskId + ".txt")
                .build();
    }

    private JSONObject workerSuccess(String taskId, String subTaskId) {
        return new JSONObject()
                .put("type", "jobDone")
                .put("taskId", taskId)
                .put("subTaskId", subTaskId)
                .put("analysis", "POS")
                .put("url", "https://example.com/a.txt")
                .put("result", "results/" + subTaskId + ".txt");
    }

    private JSONObject workerFailure(String taskId, String subTaskId) {
        return new JSONObject()
                .put("type", "failedjob")
                .put("taskId", taskId)
                .put("subTaskId", subTaskId)
                .put("analysis", "POS")
                .put("url", "https://example.com/a.txt")
                .put("error", "failed");
    }

    private static final class FakeStorage implements StorageGateway {
        private final Map<String, String> objects = new HashMap<>();
        private int putCount;

        @Override
        public String readObjectAsString(String bucket, String key) {
            return objects.get(bucket + "/" + key);
        }

        @Override
        public void putHtml(String bucket, String key, String html) {
            putCount++;
            objects.put(bucket + "/" + key, html);
        }
    }

    private static final class FakeQueues implements QueueGateway {
        private final List<JSONObject> workerTasks = new ArrayList<>();
        private final List<JSONObject> localCompletions = new ArrayList<>();
        private boolean failAfterWorkerSend;
        private boolean failLocalSend;

        @Override
        public void sendWorkerTask(JSONObject task) {
            workerTasks.add(task);
            if (failAfterWorkerSend) {
                throw new RuntimeException("crashed after send");
            }
        }

        @Override
        public void sendLocalCompletion(JSONObject completion) {
            localCompletions.add(completion);
            if (failLocalSend) {
                throw new RuntimeException("local notification failed");
            }
        }
    }

    private static final class FailingCreateStore extends InMemoryJobStateStore {
        @Override
        public synchronized boolean createJobIfAbsent(JobRecord job) {
            throw new RuntimeException("persistence unavailable");
        }
    }

    private static final class FailingResultStore extends InMemoryJobStateStore {
        @Override
        public synchronized TerminalResultStatus acceptTerminalResult(WorkerTerminalResult result, Instant now) {
            throw new RuntimeException("result persistence unavailable");
        }
    }

    private static final class FailingNotificationMarkStore extends InMemoryJobStateStore {
        private final InMemoryJobStateStore delegate;

        private FailingNotificationMarkStore(InMemoryJobStateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized boolean createJobIfAbsent(JobRecord job) {
            return delegate.createJobIfAbsent(job);
        }

        @Override
        public synchronized java.util.Optional<JobRecord> loadJob(String taskId) {
            return delegate.loadJob(taskId);
        }

        @Override
        public synchronized List<JobRecord> listRecoverableJobs(Instant now) {
            return delegate.listRecoverableJobs(now);
        }

        @Override
        public synchronized boolean claimJobLease(String taskId, String managerId, Instant now, Duration leaseDuration) {
            return delegate.claimJobLease(taskId, managerId, now, leaseDuration);
        }

        @Override
        public synchronized boolean markCompletionNotificationSent(String taskId, Instant now) {
            throw new RuntimeException("crashed before notification mark");
        }
    }
}
