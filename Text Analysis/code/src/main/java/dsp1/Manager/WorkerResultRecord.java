package dsp1.Manager;

public final class WorkerResultRecord {
    private final String taskId;
    private final String subTaskId;
    private final String analysis;
    private final String url;
    private final String outputOrError;
    private final boolean success;

    public WorkerResultRecord(String taskId,
            String subTaskId,
            String analysis,
            String url,
            String outputOrError,
            boolean success) {
        this.taskId = taskId;
        this.subTaskId = subTaskId;
        this.analysis = analysis;
        this.url = url;
        this.outputOrError = outputOrError;
        this.success = success;
    }

    public String taskId() {
        return taskId;
    }

    public String subTaskId() {
        return subTaskId;
    }

    public String analysis() {
        return analysis;
    }

    public String url() {
        return url;
    }

    public String outputOrError() {
        return outputOrError;
    }

    public boolean success() {
        return success;
    }

    public String[] toSummaryRow() {
        return new String[] { subTaskId, analysis, url, outputOrError };
    }
}
