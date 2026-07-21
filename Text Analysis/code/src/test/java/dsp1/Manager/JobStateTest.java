package dsp1.Manager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStateTest {

    @AfterEach
    void clearManagerState() {
        Manager.clearJobsForTest();
    }

    @Test
    void duplicateSuccessCountsOnce() {
        JobState state = new JobState("job-1", List.of("job-1:0"));
        WorkerResultRecord success = success("job-1", "job-1:0", "result-a");

        assertTrue(state.acceptTerminalResult(success));
        assertFalse(state.acceptTerminalResult(success));

        assertEquals(1, state.completedCount());
        assertTrue(state.isComplete());
    }

    @Test
    void duplicateFailureCountsOnce() {
        JobState state = new JobState("job-1", List.of("job-1:0"));
        WorkerResultRecord failure = failure("job-1", "job-1:0", "failed");

        assertTrue(state.acceptTerminalResult(failure));
        assertFalse(state.acceptTerminalResult(failure));

        assertEquals(1, state.completedCount());
        assertTrue(state.isComplete());
    }

    @Test
    void firstTerminalResultWinsConflict() {
        JobState state = new JobState("job-1", List.of("job-1:0"));

        assertTrue(state.acceptTerminalResult(failure("job-1", "job-1:0", "failed first")));
        assertFalse(state.acceptTerminalResult(success("job-1", "job-1:0", "late success")));

        assertEquals("ERROR: failed first", state.summaryRows().get(0)[3]);
    }

    @Test
    void mixedSuccessAndFailureCompleteTheJob() {
        JobState state = new JobState("job-1", List.of("job-1:0", "job-1:1"));

        assertTrue(state.acceptTerminalResult(success("job-1", "job-1:0", "result-a")));
        assertFalse(state.isComplete());
        assertTrue(state.acceptTerminalResult(failure("job-1", "job-1:1", "failed")));

        assertTrue(state.isComplete());
        assertEquals(2, state.summaryRows().size());
    }

    @Test
    void twoIdenticalInputLinesAreTrackedSeparately() {
        List<WorkerTask> tasks = InputTaskParser.parse("""
                POS\thttps://example.com/a.txt
                POS\thttps://example.com/a.txt
                """, "job-1");
        JobState state = JobState.fromTasks("job-1", tasks);

        assertTrue(state.acceptTerminalResult(success("job-1", "job-1:0", "result-a")));
        assertFalse(state.isComplete());
        assertTrue(state.acceptTerminalResult(success("job-1", "job-1:1", "result-b")));

        assertTrue(state.isComplete());
        assertEquals(2, state.completedCount());
    }

    @Test
    void zeroValidTasksAreImmediatelyComplete() {
        JobState state = new JobState("job-empty", List.of());

        assertTrue(state.isComplete());
        assertEquals(0, state.summaryRows().size());
    }

    @Test
    void multipleConcurrentJobsRemainIsolated() {
        JobState first = new JobState("job-1", List.of("job-1:0"));
        JobState second = new JobState("job-2", List.of("job-2:0"));

        assertTrue(first.acceptTerminalResult(success("job-1", "job-1:0", "result-a")));
        assertFalse(second.isComplete());
        assertTrue(second.acceptTerminalResult(success("job-2", "job-2:0", "result-b")));

        assertTrue(first.isComplete());
        assertTrue(second.isComplete());
    }

    @Test
    void unknownTaskIdOrSubTaskIdIsIgnored() {
        Manager.putJobForTest(new JobState("job-1", List.of("job-1:0")));

        assertFalse(Manager.acceptWorkerResult(success("missing-job", "missing-job:0", "result")));
        assertFalse(Manager.acceptWorkerResult(success("job-1", "job-1:99", "result")));
    }

    @Test
    void finalizationCanBeClaimedOnlyOnceEvenConcurrently() throws Exception {
        JobState state = new JobState("job-1", List.of());
        AtomicInteger claims = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);

        for (int i = 0; i < 16; i++) {
            executor.submit(() -> {
                try {
                    start.await();
                    if (state.markFinalizing()) {
                        claims.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, claims.get());
    }

    private static WorkerResultRecord success(String taskId, String subTaskId, String result) {
        return new WorkerResultRecord(taskId, subTaskId, "POS", "https://example.com/a.txt", result, true);
    }

    private static WorkerResultRecord failure(String taskId, String subTaskId, String error) {
        return new WorkerResultRecord(taskId, subTaskId, "POS", "https://example.com/a.txt", "ERROR: " + error, false);
    }
}
