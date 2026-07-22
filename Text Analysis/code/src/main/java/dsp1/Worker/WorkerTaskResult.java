package dsp1.Worker;

public final class WorkerTaskResult {
    private final WorkerTaskRequest request;
    private final boolean success;
    private final String resultKey;
    private final String error;

    private WorkerTaskResult(WorkerTaskRequest request, boolean success, String resultKey, String error) {
        this.request = request;
        this.success = success;
        this.resultKey = resultKey;
        this.error = error;
    }

    public static WorkerTaskResult success(WorkerTaskRequest request, String resultKey) {
        return new WorkerTaskResult(request, true, resultKey, "");
    }

    public static WorkerTaskResult failure(WorkerTaskRequest request, String error) {
        return new WorkerTaskResult(request, false, "", error);
    }

    public WorkerTaskRequest request() {
        return request;
    }

    public boolean success() {
        return success;
    }

    public String resultKey() {
        return resultKey;
    }

    public String error() {
        return error;
    }
}
