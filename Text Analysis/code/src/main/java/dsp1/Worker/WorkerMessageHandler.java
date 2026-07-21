package dsp1.Worker;

import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;

public final class WorkerMessageHandler {

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

    public static boolean handle(Message message,
            TaskProcessor processor,
            TerminalReporter reporter,
            MessageDeleter deleter) {
        WorkerTaskRequest request;
        try {
            request = parse(message);
        } catch (Exception e) {
            System.err.println("Unable to parse worker task message: " + e.getMessage());
            e.printStackTrace(System.err);
            return false;
        }

        WorkerTaskResult result;
        try {
            result = processor.process(request);
        } catch (Exception e) {
            System.err.println("Unexpected error while processing task: " + e.getMessage());
            e.printStackTrace(System.err);
            result = WorkerTaskResult.failure(request, e.getMessage());
        }

        if (result.success()) {
            reporter.sendSuccess(result);
        } else {
            reporter.sendFailure(result);
        }

        deleter.delete(message);
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
}
