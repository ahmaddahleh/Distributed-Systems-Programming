package dsp1.Manager;

import software.amazon.awssdk.services.sqs.model.Message;

public final class ManagerWorkerResultAcknowledger {

    private ManagerWorkerResultAcknowledger() {
    }

    @FunctionalInterface
    public interface ResultProcessor {
        void process(Message message);
    }

    @FunctionalInterface
    public interface MessageDeleter {
        void delete(Message message);
    }

    public static boolean handle(Message message, ResultProcessor processor, MessageDeleter deleter) {
        processor.process(message);
        deleter.delete(message);
        return true;
    }
}
