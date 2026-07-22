package dsp1.Manager;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableLocalMessageHandlerTest {

    @Test
    void successfulPersistenceIsFollowedByAcknowledgement() {
        List<String> events = new ArrayList<>();
        Message message = newTaskMessage("job-1");

        boolean handled = DurableLocalMessageHandler.persistThenAcknowledge(
                message,
                request -> events.add("persist:" + request.getString("taskId")),
                msg -> events.add("delete:" + msg.receiptHandle()));

        assertTrue(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of("persist:job-1", "delete:receipt-1"), events);
    }

    @Test
    void persistenceFailureDoesNotAcknowledge() {
        List<String> events = new ArrayList<>();

        assertThrows(RuntimeException.class, () -> DurableLocalMessageHandler.persistThenAcknowledge(
                newTaskMessage("job-1"),
                request -> {
                    events.add("persist-attempt");
                    throw new RuntimeException("store unavailable");
                },
                msg -> events.add("delete")));

        org.junit.jupiter.api.Assertions.assertEquals(List.of("persist-attempt"), events);
    }

    @Test
    void duplicateRequestCanStillBeAcknowledgedAfterStoreConfirmsItExists() {
        List<String> events = new ArrayList<>();

        DurableLocalMessageHandler.persistThenAcknowledge(
                newTaskMessage("job-1"),
                request -> events.add("already-exists:" + request.getString("taskId")),
                msg -> events.add("delete:" + msg.receiptHandle()));

        org.junit.jupiter.api.Assertions.assertEquals(List.of("already-exists:job-1", "delete:receipt-1"), events);
    }

    @Test
    void retryAfterPersistenceBeforeAcknowledgementIsSafe() {
        List<String> events = new ArrayList<>();
        Message message = newTaskMessage("job-1");

        assertThrows(RuntimeException.class, () -> DurableLocalMessageHandler.persistThenAcknowledge(
                message,
                request -> events.add("persisted:" + request.getString("taskId")),
                msg -> {
                    events.add("delete-attempt");
                    throw new RuntimeException("crash before ack completed");
                }));

        DurableLocalMessageHandler.persistThenAcknowledge(
                message,
                request -> events.add("already-exists:" + request.getString("taskId")),
                msg -> events.add("delete:" + msg.receiptHandle()));

        org.junit.jupiter.api.Assertions.assertEquals(List.of(
                "persisted:job-1",
                "delete-attempt",
                "already-exists:job-1",
                "delete:receipt-1"), events);
    }

    private static Message newTaskMessage(String taskId) {
        return Message.builder()
                .body("""
                        {"type":"newTask","taskId":"%s","s3Bucket":"bucket","key":"input","outputFile":"output.html"}
                        """.formatted(taskId))
                .receiptHandle("receipt-1")
                .build();
    }
}
