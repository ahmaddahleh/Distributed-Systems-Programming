package dsp1.Worker;

public final class WorkerTaskRequest {
    private final String taskId;
    private final String subTaskId;
    private final String sourceUrl;
    private final String analysisType;

    public WorkerTaskRequest(String taskId, String subTaskId, String sourceUrl, String analysisType) {
        this.taskId = taskId;
        this.subTaskId = subTaskId;
        this.sourceUrl = sourceUrl;
        this.analysisType = analysisType;
    }

    public String taskId() {
        return taskId;
    }

    public String subTaskId() {
        return subTaskId;
    }

    public String sourceUrl() {
        return sourceUrl;
    }

    public String analysisType() {
        return analysisType;
    }
}
