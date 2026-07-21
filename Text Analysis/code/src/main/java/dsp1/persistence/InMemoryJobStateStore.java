package dsp1.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryJobStateStore implements JobStateStore {
    private final Map<String, JobRecord> jobs = new ConcurrentHashMap<>();
    private final Map<String, Map<String, SubtaskRecord>> subtasks = new ConcurrentHashMap<>();

    @Override
    public synchronized boolean createJobIfAbsent(JobRecord job) {
        if (jobs.containsKey(job.taskId())) {
            return false;
        }
        jobs.put(job.taskId(), job);
        subtasks.put(job.taskId(), new ConcurrentHashMap<>());
        return true;
    }

    @Override
    public synchronized Optional<JobRecord> loadJob(String taskId) {
        return Optional.ofNullable(jobs.get(taskId));
    }

    @Override
    public synchronized void saveSubtasksIfAbsent(String taskId, List<SubtaskRecord> records) {
        Map<String, SubtaskRecord> byId = subtasks.computeIfAbsent(taskId, ignored -> new ConcurrentHashMap<>());
        for (SubtaskRecord record : records) {
            byId.putIfAbsent(record.subTaskId(), record);
        }
        jobs.computeIfPresent(taskId, (ignored, job) -> job.toBuilder()
                .status(JobStatus.DISPATCHING)
                .updatedAt(records.isEmpty() ? job.updatedAt() : records.get(records.size() - 1).updatedAt())
                .version(job.version() + 1)
                .build());
    }

    @Override
    public synchronized void markInputParsingComplete(String taskId, int expectedSubtaskCount, Instant now) {
        jobs.computeIfPresent(taskId, (ignored, job) -> job.toBuilder()
                .expectedSubtaskCount(expectedSubtaskCount)
                .inputParsingComplete(true)
                .status(JobStatus.RUNNING)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
    }

    @Override
    public synchronized List<SubtaskRecord> listSubtasks(String taskId) {
        return subtasks.getOrDefault(taskId, Map.of()).values().stream()
                .sorted(Comparator.comparing(SubtaskRecord::subTaskId))
                .toList();
    }

    @Override
    public synchronized List<JobRecord> listRecoverableJobs(Instant now) {
        List<JobRecord> recoverable = new ArrayList<>();
        for (JobRecord job : jobs.values()) {
            if (job.isRecoverable()) {
                recoverable.add(job);
            }
        }
        recoverable.sort(Comparator.comparing(JobRecord::createdAt));
        return recoverable;
    }

    @Override
    public synchronized boolean claimJobLease(String taskId, String managerId, Instant now, Duration leaseDuration) {
        JobRecord job = jobs.get(taskId);
        if (job == null || !leaseAvailable(job, managerId, now)) {
            return false;
        }
        jobs.put(taskId, job.toBuilder()
                .leaseOwner(managerId)
                .leaseExpiresAt(now.plus(leaseDuration))
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    @Override
    public synchronized boolean markDispatchAttempt(String taskId, String subTaskId, Instant now) {
        Map<String, SubtaskRecord> byId = subtasks.get(taskId);
        if (byId == null) {
            return false;
        }
        SubtaskRecord record = byId.get(subTaskId);
        if (record == null || record.status().isTerminal()) {
            return false;
        }
        byId.put(subTaskId, record.toBuilder()
                .status(SubtaskStatus.DISPATCHED)
                .attemptCount(record.attemptCount() + 1)
                .dispatchedAt(now)
                .updatedAt(now)
                .build());
        jobs.computeIfPresent(taskId, (ignored, job) -> job.toBuilder()
                .status(JobStatus.RUNNING)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    @Override
    public synchronized TerminalResultStatus acceptTerminalResult(WorkerTerminalResult result, Instant now) {
        JobRecord job = jobs.get(result.taskId());
        Map<String, SubtaskRecord> byId = subtasks.get(result.taskId());
        if (job == null || byId == null) {
            return TerminalResultStatus.UNKNOWN_JOB_OR_SUBTASK;
        }
        SubtaskRecord record = byId.get(result.subTaskId());
        if (record == null) {
            return TerminalResultStatus.UNKNOWN_JOB_OR_SUBTASK;
        }
        if (record.status().isTerminal()) {
            return TerminalResultStatus.DUPLICATE_OR_CONFLICT;
        }
        SubtaskStatus terminalStatus = result.success() ? SubtaskStatus.SUCCEEDED : SubtaskStatus.FAILED;
        byId.put(result.subTaskId(), record.toBuilder()
                .status(terminalStatus)
                .resultS3Key(result.resultS3Key())
                .errorMessage(result.errorMessage())
                .updatedAt(now)
                .build());
        jobs.put(result.taskId(), job.toBuilder()
                .completedSubtaskCount(job.completedSubtaskCount() + 1)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return TerminalResultStatus.ACCEPTED;
    }

    @Override
    public synchronized boolean claimFinalization(String taskId, String managerId, Instant now, Duration leaseDuration) {
        JobRecord job = jobs.get(taskId);
        if (job == null || !job.inputParsingComplete()) {
            return false;
        }
        boolean canFinalizeRunning = job.status() == JobStatus.RUNNING
                && job.completedSubtaskCount() == job.expectedSubtaskCount();
        boolean canTakeOverFinalizing = job.status() == JobStatus.FINALIZING
                && job.leaseExpiresAt().isBefore(now);
        if (!canFinalizeRunning && !canTakeOverFinalizing) {
            return false;
        }
        jobs.put(taskId, job.toBuilder()
                .status(JobStatus.FINALIZING)
                .leaseOwner(managerId)
                .leaseExpiresAt(now.plus(leaseDuration))
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    @Override
    public synchronized boolean markJobCompleted(String taskId, String managerId, String finalReportKey, Instant now) {
        JobRecord job = jobs.get(taskId);
        if (job == null || job.status() != JobStatus.FINALIZING || !managerId.equals(job.leaseOwner())) {
            return false;
        }
        jobs.put(taskId, job.toBuilder()
                .status(JobStatus.COMPLETED)
                .finalReportKey(finalReportKey)
                .notificationStatus(NotificationStatus.PENDING)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    @Override
    public synchronized boolean markCompletionNotificationSent(String taskId, Instant now) {
        JobRecord job = jobs.get(taskId);
        if (job == null || job.status() != JobStatus.COMPLETED) {
            return false;
        }
        jobs.put(taskId, job.toBuilder()
                .notificationStatus(NotificationStatus.SENT)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    @Override
    public synchronized boolean failJob(String taskId, String failureReason, Instant now) {
        JobRecord job = jobs.get(taskId);
        if (job == null) {
            return false;
        }
        jobs.put(taskId, job.toBuilder()
                .status(JobStatus.FAILED)
                .failureReason(failureReason)
                .updatedAt(now)
                .version(job.version() + 1)
                .build());
        return true;
    }

    private static boolean leaseAvailable(JobRecord job, String managerId, Instant now) {
        return job.leaseOwner().isBlank()
                || managerId.equals(job.leaseOwner())
                || job.leaseExpiresAt().isBefore(now);
    }
}
