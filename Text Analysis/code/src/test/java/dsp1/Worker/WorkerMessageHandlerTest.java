package dsp1.Worker;

import dsp1.persistence.ClaimedSubtaskCompletionStatus;
import dsp1.persistence.ProcessingLeaseRenewalStatus;
import dsp1.persistence.SubtaskClaimStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerMessageHandlerTest {

    @Test
    void successfulProcessingDeletesAfterResultNotification() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handle(
                message,
                request -> {
                    events.add("process:" + request.subTaskId());
                    return WorkerTaskResult.success(request, "results/job-1/job-1_0.txt");
                },
                new RecordingReporter(events),
                msg -> events.add("delete:" + msg.receiptHandle()));

        assertTrue(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of(
                "process:job-1:0",
                "success:job-1:0",
                "delete:receipt-1"), events);
    }

    @Test
    void processingFailureSendsFailureBeforeDelete() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handle(
                message,
                request -> WorkerTaskResult.failure(request, "download failed"),
                new RecordingReporter(events),
                msg -> events.add("delete:" + msg.receiptHandle()));

        assertTrue(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of(
                "failure:job-1:0",
                "delete:receipt-1"), events);
    }

    @Test
    void resultNotificationFailureDoesNotDeleteOriginalMessage() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        assertThrows(RuntimeException.class, () -> WorkerMessageHandler.handle(
                message,
                request -> WorkerTaskResult.success(request, "results/job-1/job-1_0.txt"),
                new WorkerMessageHandler.TerminalReporter() {
                    @Override
                    public void sendSuccess(WorkerTaskResult result) {
                        events.add("success-attempt");
                        throw new RuntimeException("sqs unavailable");
                    }

                    @Override
                    public void sendFailure(WorkerTaskResult result) {
                        events.add("failure-attempt");
                    }
                },
                msg -> events.add("delete")));

        org.junit.jupiter.api.Assertions.assertEquals(List.of("success-attempt"), events);
    }

    @Test
    void queueMessageDeleterUsesResolvedQueueUrl() {
        Message message = taskMessage();
        List<String> deletes = new ArrayList<>();
        QueueMessageDeleter deleter = new QueueMessageDeleter(
                "https://sqs.us-east-1.amazonaws.com/123/ManagerToWorkersQueue",
                (queueUrl, msg) -> deletes.add(queueUrl + "|" + msg.receiptHandle()));

        deleter.delete(message);

        org.junit.jupiter.api.Assertions.assertEquals(List.of(
                "https://sqs.us-east-1.amazonaws.com/123/ManagerToWorkersQueue|receipt-1"), deletes);
    }

    @Test
    void invalidMessageIsNotDeleted() {
        Message message = Message.builder()
                .body("{\"type\":\"workerTask\"}")
                .receiptHandle("receipt-1")
                .build();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handle(
                message,
                request -> WorkerTaskResult.success(request, "unused"),
                new RecordingReporter(events),
                msg -> events.add("delete"));

        assertFalse(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), events);
    }

    @Test
    void duplicateMessageOwnedByAnotherWorkerDoesNotProcessOrDelete() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handleWithLease(
                message,
                request -> {
                    events.add("process");
                    return WorkerTaskResult.success(request, "unused");
                },
                new RecordingReporter(events),
                msg -> events.add("delete"),
                msg -> events.add("delay:" + msg.receiptHandle()),
                new FakeLeaseCoordinator(SubtaskClaimStatus.OWNED_BY_ANOTHER_WORKER),
                (request, coordinator) -> new RecordingHeartbeat(events));

        assertFalse(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of("delay:receipt-1"), events);
    }

    @Test
    void temporaryLeaseFailureDoesNotStartProcessingOrDelete() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handleWithLease(
                message,
                request -> {
                    events.add("process");
                    return WorkerTaskResult.success(request, "unused");
                },
                new RecordingReporter(events),
                msg -> events.add("delete"),
                msg -> events.add("delay"),
                new ThrowingClaimCoordinator(),
                (request, coordinator) -> new RecordingHeartbeat(events));

        assertFalse(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of(), events);
    }

    @Test
    void terminalDuplicateMessageDeletesWithoutProcessing() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handleWithLease(
                message,
                request -> {
                    events.add("process");
                    return WorkerTaskResult.success(request, "unused");
                },
                new RecordingReporter(events),
                msg -> events.add("delete:" + msg.receiptHandle()),
                msg -> events.add("delay"),
                new FakeLeaseCoordinator(SubtaskClaimStatus.ALREADY_TERMINAL),
                (request, coordinator) -> new RecordingHeartbeat(events));

        assertTrue(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of("delete:receipt-1"), events);
    }

    @Test
    void staleOwnerCompletionDoesNotNotifyOrDelete() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();
        FakeLeaseCoordinator coordinator = new FakeLeaseCoordinator(SubtaskClaimStatus.CLAIMED);
        coordinator.completionStatus = ClaimedSubtaskCompletionStatus.STALE_OWNER;

        boolean handled = WorkerMessageHandler.handleWithLease(
                message,
                request -> {
                    events.add("process:" + request.subTaskId());
                    return WorkerTaskResult.success(request, "results/job-1/job-1_0.txt");
                },
                new RecordingReporter(events),
                msg -> events.add("delete"),
                msg -> events.add("delay"),
                coordinator,
                (request, ignored) -> new RecordingHeartbeat(events));

        assertFalse(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of("process:job-1:0", "heartbeat-closed"), events);
    }

    @Test
    void heartbeatOwnershipLossDoesNotNotifyOrDelete() {
        Message message = taskMessage();
        List<String> events = new ArrayList<>();

        boolean handled = WorkerMessageHandler.handleWithLease(
                message,
                request -> {
                    events.add("process:" + request.subTaskId());
                    return WorkerTaskResult.success(request, "results/job-1/job-1_0.txt");
                },
                new RecordingReporter(events),
                msg -> events.add("delete"),
                msg -> events.add("delay"),
                new FakeLeaseCoordinator(SubtaskClaimStatus.CLAIMED),
                (request, ignored) -> new RecordingHeartbeat(events, true));

        assertFalse(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of("process:job-1:0", "heartbeat-closed"), events);
    }

    private static Message taskMessage() {
        return Message.builder()
                .body("""
                        {"type":"workerTask","taskId":"job-1","subTaskId":"job-1:0","analysis":"POS","url":"https://example.com/a.txt"}
                        """)
                .receiptHandle("receipt-1")
                .build();
    }

    private static class RecordingReporter implements WorkerMessageHandler.TerminalReporter {
        private final List<String> events;

        private RecordingReporter(List<String> events) {
            this.events = events;
        }

        @Override
        public void sendSuccess(WorkerTaskResult result) {
            events.add("success:" + result.request().subTaskId());
        }

        @Override
        public void sendFailure(WorkerTaskResult result) {
            events.add("failure:" + result.request().subTaskId());
        }
    }

    private static class FakeLeaseCoordinator implements WorkerMessageHandler.ProcessingLeaseCoordinator {
        private final SubtaskClaimStatus claimStatus;
        private ClaimedSubtaskCompletionStatus completionStatus = ClaimedSubtaskCompletionStatus.COMPLETED;

        private FakeLeaseCoordinator(SubtaskClaimStatus claimStatus) {
            this.claimStatus = claimStatus;
        }

        @Override
        public SubtaskClaimStatus claim(WorkerTaskRequest request) {
            return claimStatus;
        }

        @Override
        public ProcessingLeaseRenewalStatus renew(WorkerTaskRequest request) {
            return ProcessingLeaseRenewalStatus.RENEWED;
        }

        @Override
        public ClaimedSubtaskCompletionStatus complete(WorkerTaskResult result) {
            return completionStatus;
        }
    }

    private static final class ThrowingClaimCoordinator extends FakeLeaseCoordinator {
        private ThrowingClaimCoordinator() {
            super(SubtaskClaimStatus.CLAIMED);
        }

        @Override
        public SubtaskClaimStatus claim(WorkerTaskRequest request) {
            throw new RuntimeException("dynamodb unavailable");
        }
    }

    private static final class RecordingHeartbeat implements WorkerMessageHandler.LeaseHeartbeat {
        private final List<String> events;
        private final boolean ownershipLost;

        private RecordingHeartbeat(List<String> events) {
            this(events, false);
        }

        private RecordingHeartbeat(List<String> events, boolean ownershipLost) {
            this.events = events;
            this.ownershipLost = ownershipLost;
        }

        @Override
        public boolean ownershipLost() {
            return ownershipLost;
        }

        @Override
        public void close() {
            events.add("heartbeat-closed");
        }
    }
}
