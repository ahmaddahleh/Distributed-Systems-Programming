package dsp1.Worker;

import dsp1.persistence.ClaimedSubtaskCompletionStatus;
import dsp1.persistence.ProcessingLeaseRenewalStatus;
import dsp1.persistence.SubtaskClaimStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

public final class WorkerMessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(WorkerMessageHandler.class);

    private WorkerMessageHandler() {
    }

    @FunctionalInterface
    public interface TaskProcessor {
        WorkerTaskResult process(WorkerTaskRequest request) throws Exception;
    }

    public interface TerminalReporter {
        void sendSuccess(WorkerTaskResult result);

        void sendFailure(WorkerTaskResult result);
    }

    @FunctionalInterface
    public interface MessageDeleter {
        void delete(Message message);
    }

    @FunctionalInterface
    public interface DuplicateMessageDelayer {
        void delay(Message message);
    }

    public interface ProcessingLeaseCoordinator {
        SubtaskClaimStatus claim(WorkerTaskRequest request);

        ProcessingLeaseRenewalStatus renew(WorkerTaskRequest request);

        ClaimedSubtaskCompletionStatus complete(WorkerTaskResult result);
    }

    public interface LeaseHeartbeat extends AutoCloseable {
        boolean ownershipLost();

        @Override
        void close();
    }

    @FunctionalInterface
    public interface LeaseHeartbeatFactory {
        LeaseHeartbeat start(WorkerTaskRequest request, ProcessingLeaseCoordinator leaseCoordinator);
    }

    public static boolean handle(Message message,
            TaskProcessor processor,
            TerminalReporter reporter,
            MessageDeleter deleter) {
        return handleWithLease(
                message,
                processor,
                reporter,
                deleter,
                ignored -> { },
                NoopProcessingLeaseCoordinator.INSTANCE,
                (request, coordinator) -> NoopLeaseHeartbeat.INSTANCE);
    }

    public static boolean handleWithLease(Message message,
            TaskProcessor processor,
            TerminalReporter reporter,
            MessageDeleter deleter,
            DuplicateMessageDelayer duplicateMessageDelayer,
            ProcessingLeaseCoordinator leaseCoordinator,
            LeaseHeartbeatFactory heartbeatFactory) {
        WorkerTaskRequest request;
        try {
            request = parse(message);
        } catch (Exception e) {
            logger.warn("component=Worker event=task_parse_failed", e);
            return false;
        }

        SubtaskClaimStatus claimStatus;
        try {
            claimStatus = leaseCoordinator.claim(request);
        } catch (RuntimeException e) {
            logger.warn("component=Worker taskId={} subTaskId={} event=processing_lease_claim_failed",
                    request.taskId(), request.subTaskId(), e);
            return false;
        }
        if (claimStatus == SubtaskClaimStatus.OWNED_BY_ANOTHER_WORKER) {
            logger.info("component=Worker taskId={} subTaskId={} event=processing_lease_owned_by_another_worker",
                    request.taskId(), request.subTaskId());
            duplicateMessageDelayer.delay(message);
            return false;
        }
        if (claimStatus == SubtaskClaimStatus.ALREADY_TERMINAL) {
            logger.info("component=Worker taskId={} subTaskId={} event=duplicate_terminal_task_message",
                    request.taskId(), request.subTaskId());
            deleter.delete(message);
            return true;
        }
        if (claimStatus == SubtaskClaimStatus.NOT_FOUND) {
            logger.warn("component=Worker taskId={} subTaskId={} event=processing_lease_subtask_not_found",
                    request.taskId(), request.subTaskId());
            return false;
        }

        WorkerTaskResult result;
        LeaseHeartbeat heartbeat = heartbeatFactory.start(request, leaseCoordinator);
        try (heartbeat) {
            result = processor.process(request);
        } catch (Exception e) {
            logger.error("component=Worker taskId={} subTaskId={} event=task_processing_exception",
                    request.taskId(), request.subTaskId(), e);
            result = WorkerTaskResult.failure(request, e.getMessage());
        }

        if (heartbeat.ownershipLost()) {
            logger.warn("component=Worker taskId={} subTaskId={} event=processing_lease_lost_before_publish",
                    request.taskId(), request.subTaskId());
            return false;
        }

        ClaimedSubtaskCompletionStatus completion;
        try {
            completion = leaseCoordinator.complete(result);
        } catch (RuntimeException e) {
            logger.warn("component=Worker taskId={} subTaskId={} event=processing_lease_completion_failed",
                    request.taskId(), request.subTaskId(), e);
            return false;
        }
        if (completion == ClaimedSubtaskCompletionStatus.STALE_OWNER
                || completion == ClaimedSubtaskCompletionStatus.NOT_FOUND) {
            logger.warn("component=Worker taskId={} subTaskId={} event=stale_processing_result completion={}",
                    request.taskId(), request.subTaskId(), completion);
            return false;
        }
        if (completion == ClaimedSubtaskCompletionStatus.ALREADY_TERMINAL) {
            logger.info("component=Worker taskId={} subTaskId={} event=terminal_result_already_recorded",
                    request.taskId(), request.subTaskId());
            deleter.delete(message);
            return true;
        }

        if (result.success()) {
            logger.info("component=Worker taskId={} subTaskId={} event=report_success",
                    request.taskId(), request.subTaskId());
            reporter.sendSuccess(result);
        } else {
            logger.info("component=Worker taskId={} subTaskId={} event=report_failure",
                    request.taskId(), request.subTaskId());
            reporter.sendFailure(result);
        }

        deleter.delete(message);
        logger.info("component=Worker taskId={} subTaskId={} event=task_message_deleted",
                request.taskId(), request.subTaskId());
        return true;
    }

    private static WorkerTaskRequest parse(Message message) {
        JSONObject payload = new JSONObject(message.body());
        String taskId = payload.getString("taskId");
        String subTaskId = payload.optString("subTaskId", taskId);
        String sourceUrl = payload.getString("url");
        String analysisType = payload.getString("analysis");
        return new WorkerTaskRequest(taskId, subTaskId, sourceUrl, analysisType);
    }

    private enum NoopLeaseHeartbeat implements LeaseHeartbeat {
        INSTANCE;

        @Override
        public boolean ownershipLost() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private enum NoopProcessingLeaseCoordinator implements ProcessingLeaseCoordinator {
        INSTANCE;

        @Override
        public SubtaskClaimStatus claim(WorkerTaskRequest request) {
            return SubtaskClaimStatus.CLAIMED;
        }

        @Override
        public ProcessingLeaseRenewalStatus renew(WorkerTaskRequest request) {
            return ProcessingLeaseRenewalStatus.RENEWED;
        }

        @Override
        public ClaimedSubtaskCompletionStatus complete(WorkerTaskResult result) {
            return ClaimedSubtaskCompletionStatus.COMPLETED;
        }
    }
}
