package dsp1.Manager;

import dsp1.persistence.JobRecord;
import dsp1.persistence.JobStateStore;
import dsp1.persistence.JobStatus;
import dsp1.persistence.NotificationStatus;
import dsp1.persistence.SubtaskRecord;
import dsp1.persistence.SubtaskStatus;
import dsp1.persistence.TerminalResultStatus;
import dsp1.persistence.WorkerTerminalResult;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class DurableManagerService {
    private static final Logger logger = LoggerFactory.getLogger(DurableManagerService.class);

    private final JobStateStore store;
    private final StorageGateway storage;
    private final QueueGateway queues;
    private final Clock clock;
    private final DurableManagerConfig config;

    public DurableManagerService(JobStateStore store,
            StorageGateway storage,
            QueueGateway queues,
            Clock clock,
            DurableManagerConfig config) {
        this.store = store;
        this.storage = storage;
        this.queues = queues;
        this.clock = clock;
        this.config = config;
    }

    public boolean persistJobRequest(JSONObject obj) {
        Instant now = clock.instant();
        String taskId = obj.getString("taskId");
        JobRecord job = JobRecord.builder(taskId)
                .inputBucket(obj.getString("s3Bucket"))
                .inputKey(obj.getString("key"))
                .outputBucket(obj.getString("s3Bucket"))
                .outputFileName(obj.optString("outputFile", "output.html"))
                .terminate(obj.optBoolean("terminate", false))
                .createdAt(now)
                .updatedAt(now)
                .build();

        boolean created = store.createJobIfAbsent(job);
        if (created) {
            logger.info("component=Manager taskId={} event=job_persisted", taskId);
        } else {
            logger.info("component=Manager taskId={} event=duplicate_job_request", taskId);
        }
        return true;
    }

    public void recoverJob(String taskId) {
        store.loadJob(taskId).ifPresent(this::recoverJob);
    }

    public void recoverAll() {
        for (JobRecord job : store.listRecoverableJobs(clock.instant())) {
            recoverJob(job);
        }
    }

    private void recoverJob(JobRecord job) {
        if (job.status() == JobStatus.FINALIZING) {
            tryFinalize(job.taskId());
            return;
        }
        if (job.status() == JobStatus.COMPLETED
                && job.notificationStatus() == NotificationStatus.PENDING) {
            retryCompletionNotification(job);
            return;
        }
        if (!store.claimJobLease(job.taskId(), config.managerId(), clock.instant(), config.leaseDuration())) {
            return;
        }

        JobRecord current = store.loadJob(job.taskId()).orElse(job);
        if (!current.inputParsingComplete()) {
            parseInputDurably(current);
            current = store.loadJob(job.taskId()).orElse(current);
        }

        if (current.status() == JobStatus.RECEIVED
                || current.status() == JobStatus.DISPATCHING
                || current.status() == JobStatus.RUNNING) {
            dispatchRecoverableSubtasks(current);
            tryFinalize(current.taskId());
        }
    }

    private void parseInputDurably(JobRecord job) {
        Instant now = clock.instant();
        String input = storage.readObjectAsString(job.inputBucket(), job.inputKey());
        List<WorkerTask> parsed = InputTaskParser.parse(input, job.taskId());
        List<SubtaskRecord> records = new ArrayList<>();
        for (WorkerTask task : parsed) {
            records.add(SubtaskRecord.builder(task.taskId(), task.subTaskId())
                    .analysis(task.analysis())
                    .url(task.url())
                    .status(SubtaskStatus.PENDING)
                    .createdAt(now)
                    .updatedAt(now)
                    .build());
        }
        store.saveSubtasksIfAbsent(job.taskId(), records);
        store.markInputParsingComplete(job.taskId(), records.size(), now);
        logger.info("component=Manager taskId={} event=input_parsing_complete expectedSubtaskCount={}",
                job.taskId(), records.size());
    }

    private void dispatchRecoverableSubtasks(JobRecord job) {
        Instant staleBefore = clock.instant().minus(config.staleDispatchTimeout());
        for (SubtaskRecord subtask : store.listSubtasks(job.taskId())) {
            Instant now = clock.instant();
            if (subtask.status() == SubtaskStatus.PENDING
                    || subtask.isStaleDispatched(staleBefore)
                    || subtask.isExpiredProcessing(now)) {
                JSONObject payload = new JSONObject()
                        .put("type", "workerTask")
                        .put("taskId", subtask.taskId())
                        .put("subTaskId", subtask.subTaskId())
                        .put("analysis", subtask.analysis())
                        .put("url", subtask.url());
                queues.sendWorkerTask(payload);
                store.markDispatchAttempt(subtask.taskId(), subtask.subTaskId(), now);
                logger.info("component=Manager taskId={} subTaskId={} event=worker_task_dispatched",
                        subtask.taskId(), subtask.subTaskId());
            }
        }
    }

    public TerminalResultStatus handleWorkerResult(JSONObject obj) {
        String taskId = obj.getString("taskId");
        String subTaskId = obj.getString("subTaskId");
        boolean success = "jobDone".equals(obj.getString("type"));
        WorkerTerminalResult result = WorkerTerminalResult.builder(taskId, subTaskId)
                .analysis(obj.optString("analysis"))
                .url(obj.optString("url"))
                .success(success)
                .resultS3Key(obj.optString("result"))
                .errorMessage(obj.optString("error"))
                .build();
        TerminalResultStatus status = store.acceptTerminalResult(result, clock.instant());
        if (status == TerminalResultStatus.ACCEPTED || status == TerminalResultStatus.DUPLICATE_OR_CONFLICT) {
            tryFinalize(taskId);
        }
        return status;
    }

    public boolean tryFinalize(String taskId) {
        Optional<JobRecord> maybeJob = store.loadJob(taskId);
        if (maybeJob.isEmpty()) {
            return false;
        }
        JobRecord job = maybeJob.get();
        if (job.status() == JobStatus.COMPLETED) {
            retryCompletionNotification(job);
            return true;
        }
        if (!job.inputParsingComplete() || job.completedSubtaskCount() != job.expectedSubtaskCount()) {
            return false;
        }
        List<SubtaskRecord> subtasks = store.listSubtasks(taskId);
        if (subtasks.stream().anyMatch(subtask -> !subtask.status().isTerminal())) {
            return false;
        }
        if (!store.claimFinalization(taskId, config.managerId(), clock.instant(), config.leaseDuration())) {
            return false;
        }
        String reportKey = deterministicReportKey(taskId);
        List<String[]> rows = subtasks.stream()
                .map(this::toSummaryRow)
                .toList();
        String html = HtmlReportBuilder.build(taskId, job.outputBucket(), rows);
        storage.putHtml(job.outputBucket(), reportKey, html);
        if (store.markJobCompleted(taskId, config.managerId(), reportKey, clock.instant())) {
            retryCompletionNotification(store.loadJob(taskId).orElse(job.toBuilder()
                    .status(JobStatus.COMPLETED)
                    .finalReportKey(reportKey)
                    .build()));
            return true;
        }
        return false;
    }

    public void retryCompletionNotification(JobRecord job) {
        if (job.status() != JobStatus.COMPLETED || job.notificationStatus() == NotificationStatus.SENT) {
            return;
        }
        JSONObject doneMsg = new JSONObject()
                .put("type", "jobDone")
                .put("taskId", job.taskId())
                .put("s3Bucket", job.outputBucket())
                .put("outputS3Key", job.finalReportKey());
        queues.sendLocalCompletion(doneMsg);
        store.markCompletionNotificationSent(job.taskId(), clock.instant());
        logger.info("component=Manager taskId={} event=completion_notification_sent", job.taskId());
    }

    public static String deterministicReportKey(String taskId) {
        return "reports/" + taskId + "/summary.html";
    }

    private String[] toSummaryRow(SubtaskRecord subtask) {
        String output = subtask.status() == SubtaskStatus.SUCCEEDED
                ? subtask.resultS3Key()
                : "ERROR: " + subtask.errorMessage();
        return new String[] { subtask.subTaskId(), subtask.analysis(), subtask.url(), output };
    }
}
