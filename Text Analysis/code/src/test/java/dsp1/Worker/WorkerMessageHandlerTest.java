package dsp1.Worker;

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
}
