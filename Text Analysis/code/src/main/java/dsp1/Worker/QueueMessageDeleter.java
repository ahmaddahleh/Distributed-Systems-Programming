package dsp1.Worker;

import software.amazon.awssdk.services.sqs.model.Message;

public final class QueueMessageDeleter implements WorkerMessageHandler.MessageDeleter {

    @FunctionalInterface
    public interface DeleteOperation {
        void delete(String queueUrl, Message message);
    }

    private final String queueUrl;
    private final DeleteOperation deleteOperation;

    public QueueMessageDeleter(String queueUrl, DeleteOperation deleteOperation) {
        this.queueUrl = queueUrl;
        this.deleteOperation = deleteOperation;
    }

    @Override
    public void delete(Message message) {
        deleteOperation.delete(queueUrl, message);
    }
}
