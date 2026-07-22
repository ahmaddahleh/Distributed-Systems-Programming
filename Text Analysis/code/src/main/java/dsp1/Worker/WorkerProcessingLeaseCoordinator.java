package dsp1.Worker;

import dsp1.persistence.ClaimedSubtaskCompletionStatus;
import dsp1.persistence.JobStateStore;
import dsp1.persistence.ProcessingLeaseRenewalStatus;
import dsp1.persistence.SubtaskClaimStatus;
import dsp1.persistence.WorkerTerminalResult;

import java.time.Clock;
import java.time.Duration;

public final class WorkerProcessingLeaseCoordinator implements WorkerMessageHandler.ProcessingLeaseCoordinator {
    private final JobStateStore store;
    private final String workerId;
    private final Clock clock;
    private final Duration leaseDuration;

    public WorkerProcessingLeaseCoordinator(JobStateStore store, String workerId, Clock clock, Duration leaseDuration) {
        this.store = store;
        this.workerId = workerId;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
    }

    @Override
    public SubtaskClaimStatus claim(WorkerTaskRequest request) {
        return store.claimSubtaskForProcessing(
                request.taskId(),
                request.subTaskId(),
                workerId,
                clock.instant(),
                leaseDuration);
    }

    @Override
    public ProcessingLeaseRenewalStatus renew(WorkerTaskRequest request) {
        return store.renewProcessingLease(
                request.taskId(),
                request.subTaskId(),
                workerId,
                clock.instant(),
                leaseDuration);
    }

    @Override
    public ClaimedSubtaskCompletionStatus complete(WorkerTaskResult result) {
        WorkerTaskRequest request = result.request();
        WorkerTerminalResult terminalResult = WorkerTerminalResult.builder(request.taskId(), request.subTaskId())
                .analysis(request.analysisType())
                .url(request.sourceUrl())
                .success(result.success())
                .resultS3Key(result.resultKey())
                .errorMessage(result.error())
                .build();
        return store.completeClaimedSubtask(terminalResult, workerId, clock.instant());
    }
}
