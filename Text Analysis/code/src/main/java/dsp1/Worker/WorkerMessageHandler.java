package dsp1.Worker;

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

    public static boolean handle(Message message,
            TaskProcessor processor,
            TerminalReporter reporter,
            MessageDeleter deleter) {
        WorkerTaskRequest request;
        try {
            request = parse(message);
        } catch (Exception e) {
            logger.warn("component=Worker event=task_parse_failed", e);
            return false;
        }

        WorkerTaskResult result;
        try {
            result = processor.process(request);
        } catch (Exception e) {
            logger.error("component=Worker taskId={} subTaskId={} event=task_processing_exception",
                    request.taskId(), request.subTaskId(), e);
            result = WorkerTaskResult.failure(request, e.getMessage());
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
}
