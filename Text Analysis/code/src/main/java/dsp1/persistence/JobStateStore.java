package dsp1.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface JobStateStore {
    boolean createJobIfAbsent(JobRecord job);

    Optional<JobRecord> loadJob(String taskId);

    void saveSubtasksIfAbsent(String taskId, List<SubtaskRecord> subtasks);

    void markInputParsingComplete(String taskId, int expectedSubtaskCount, Instant now);

    List<SubtaskRecord> listSubtasks(String taskId);

    List<JobRecord> listRecoverableJobs(Instant now);

    boolean claimJobLease(String taskId, String managerId, Instant now, Duration leaseDuration);

    boolean markDispatchAttempt(String taskId, String subTaskId, Instant now);

    TerminalResultStatus acceptTerminalResult(WorkerTerminalResult result, Instant now);

    boolean claimFinalization(String taskId, String managerId, Instant now, Duration leaseDuration);

    boolean markJobCompleted(String taskId, String managerId, String finalReportKey, Instant now);

    boolean markCompletionNotificationSent(String taskId, Instant now);

    boolean failJob(String taskId, String failureReason, Instant now);
}
