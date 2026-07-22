package dsp1.persistence;

public final class WorkerTerminalResult {
    private final String taskId;
    private final String subTaskId;
    private final String analysis;
    private final String url;
    private final boolean success;
    private final String resultS3Key;
    private final String errorMessage;

    private WorkerTerminalResult(Builder builder) {
        this.taskId = builder.taskId;
        this.subTaskId = builder.subTaskId;
        this.analysis = builder.analysis;
        this.url = builder.url;
        this.success = builder.success;
        this.resultS3Key = builder.resultS3Key;
        this.errorMessage = builder.errorMessage;
    }

    public static Builder builder(String taskId, String subTaskId) {
        return new Builder(taskId, subTaskId);
    }

    public String taskId() { return taskId; }
    public String subTaskId() { return subTaskId; }
    public String analysis() { return analysis; }
    public String url() { return url; }
    public boolean success() { return success; }
    public String resultS3Key() { return resultS3Key; }
    public String errorMessage() { return errorMessage; }

    public static final class Builder {
        private final String taskId;
        private final String subTaskId;
        private String analysis = "";
        private String url = "";
        private boolean success;
        private String resultS3Key = "";
        private String errorMessage = "";

        private Builder(String taskId, String subTaskId) {
            this.taskId = taskId;
            this.subTaskId = subTaskId;
        }

        public Builder analysis(String analysis) { this.analysis = analysis; return this; }
        public Builder url(String url) { this.url = url; return this; }
        public Builder success(boolean success) { this.success = success; return this; }
        public Builder resultS3Key(String resultS3Key) { this.resultS3Key = resultS3Key; return this; }
        public Builder errorMessage(String errorMessage) { this.errorMessage = errorMessage; return this; }

        public WorkerTerminalResult build() {
            return new WorkerTerminalResult(this);
        }
    }
}
