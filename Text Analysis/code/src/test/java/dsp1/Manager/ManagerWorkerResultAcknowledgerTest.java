package dsp1.Manager;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagerWorkerResultAcknowledgerTest {

    @Test
    void deletesWorkerResultOnlyAfterSuccessfulAggregation() {
        Message message = Message.builder().receiptHandle("worker-result-receipt").build();
        List<String> events = new ArrayList<>();

        boolean handled = ManagerWorkerResultAcknowledger.handle(
                message,
                msg -> events.add("aggregate:" + msg.receiptHandle()),
                msg -> events.add("delete:" + msg.receiptHandle()));

        assertTrue(handled);
        org.junit.jupiter.api.Assertions.assertEquals(List.of(
                "aggregate:worker-result-receipt",
                "delete:worker-result-receipt"), events);
    }

    @Test
    void aggregationFailureDoesNotDeleteWorkerResultMessage() {
        Message message = Message.builder().receiptHandle("worker-result-receipt").build();
        List<String> events = new ArrayList<>();

        assertThrows(RuntimeException.class, () -> ManagerWorkerResultAcknowledger.handle(
                message,
                msg -> {
                    events.add("aggregate-attempt");
                    throw new RuntimeException("state update failed");
                },
                msg -> events.add("delete")));

        org.junit.jupiter.api.Assertions.assertEquals(List.of("aggregate-attempt"), events);
    }

    @Test
    void deleterReceivesTheOriginalMessageReceiptHandle() {
        Message message = Message.builder().receiptHandle("expected-receipt").build();
        List<String> deletedHandles = new ArrayList<>();

        ManagerWorkerResultAcknowledger.handle(
                message,
                msg -> {
                },
                msg -> deletedHandles.add(msg.receiptHandle()));

        org.junit.jupiter.api.Assertions.assertEquals(List.of("expected-receipt"), deletedHandles);
    }
}
