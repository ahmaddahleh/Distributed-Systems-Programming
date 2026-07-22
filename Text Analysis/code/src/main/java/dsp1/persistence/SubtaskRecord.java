package dsp1.persistence;

import java.time.Instant;

public final class SubtaskRecord {
    private final String taskId;
    private final String subTaskId;
    private final String analysis;
    private final String url;
    private final SubtaskStatus status;
    private final String resultS3Key;
    private final String errorMessage;
    private final int attemptCount;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Instant dispatchedAt;
    private final String processingOwner;
    private final Instant processingLeaseExpiresAt;
    private final Instant processingStartedAt;
    private final Instant lastHeartbeatAt;

    private SubtaskRecord(Builder builder) {
        this.taskId = builder.taskId;
        this.subTaskId = builder.subTaskId;
        this.analysis = builder.analysis;
        this.url = builder.url;
        this.status = builder.status;
        this.resultS3Key = builder.resultS3Key;
        this.errorMessage = builder.errorMessage;
        this.attemptCount = builder.attemptCount;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.dispatchedAt = builder.dispatchedAt;
        this.processingOwner = builder.processingOwner;
        this.processingLeaseExpiresAt = builder.processingLeaseExpiresAt;
        this.processingStartedAt = builder.processingStartedAt;
        this.lastHeartbeatAt = builder.lastHeartbeatAt;
    }

    public static Builder builder(String taskId, String subTaskId) {
        return new Builder(taskId, subTaskId);
    }

    public Builder toBuilder() {
        return new Builder(taskId, subTaskId)
                .analysis(analysis)
                .url(url)
                .status(status)
                .resultS3Key(resultS3Key)
                .errorMessage(errorMessage)
                .attemptCount(attemptCount)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .dispatchedAt(dispatchedAt)
                .processingOwner(processingOwner)
                .processingLeaseExpiresAt(processingLeaseExpiresAt)
                .processingStartedAt(processingStartedAt)
                .lastHeartbeatAt(lastHeartbeatAt);
    }

    public String taskId() { return taskId; }
    public String subTaskId() { return subTaskId; }
    public String analysis() { return analysis; }
    public String url() { return url; }
    public SubtaskStatus status() { return status; }
    public String resultS3Key() { return resultS3Key; }
    public String errorMessage() { return errorMessage; }
    public int attemptCount() { return attemptCount; }
    public Instant createdAt() { return createdAt; }
    public Instant updatedAt() { return updatedAt; }
    public Instant dispatchedAt() { return dispatchedAt; }
    public String processingOwner() { return processingOwner; }
    public Instant processingLeaseExpiresAt() { return processingLeaseExpiresAt; }
    public Instant processingStartedAt() { return processingStartedAt; }
    public Instant lastHeartbeatAt() { return lastHeartbeatAt; }

    public boolean isStaleDispatched(Instant staleBefore) {
        return status == SubtaskStatus.DISPATCHED && dispatchedAt.isBefore(staleBefore);
    }

    public boolean isExpiredProcessing(Instant now) {
        return status == SubtaskStatus.PROCESSING && processingLeaseExpiresAt.isBefore(now);
    }

    public static final class Builder {
        private final String taskId;
        private final String subTaskId;
        private String analysis = "";
        private String url = "";
        private SubtaskStatus status = SubtaskStatus.PENDING;
        private String resultS3Key = "";
        private String errorMessage = "";
        private int attemptCount;
        private Instant createdAt = Instant.EPOCH;
        private Instant updatedAt = Instant.EPOCH;
        private Instant dispatchedAt = Instant.EPOCH;
        private String processingOwner = "";
        private Instant processingLeaseExpiresAt = Instant.EPOCH;
        private Instant processingStartedAt = Instant.EPOCH;
        private Instant lastHeartbeatAt = Instant.EPOCH;

        private Builder(String taskId, String subTaskId) {
            this.taskId = taskId;
            this.subTaskId = subTaskId;
        }

        public Builder analysis(String analysis) { this.analysis = analysis; return this; }
        public Builder url(String url) { this.url = url; return this; }
        public Builder status(SubtaskStatus status) { this.status = status; return this; }
        public Builder resultS3Key(String resultS3Key) { this.resultS3Key = resultS3Key; return this; }
        public Builder errorMessage(String errorMessage) { this.errorMessage = errorMessage; return this; }
        public Builder attemptCount(int attemptCount) { this.attemptCount = attemptCount; return this; }
        public Builder createdAt(Instant createdAt) { this.createdAt = createdAt; return this; }
        public Builder updatedAt(Instant updatedAt) { this.updatedAt = updatedAt; return this; }
        public Builder dispatchedAt(Instant dispatchedAt) { this.dispatchedAt = dispatchedAt; return this; }
        public Builder processingOwner(String processingOwner) { this.processingOwner = processingOwner; return this; }
        public Builder processingLeaseExpiresAt(Instant processingLeaseExpiresAt) { this.processingLeaseExpiresAt = processingLeaseExpiresAt; return this; }
        public Builder processingStartedAt(Instant processingStartedAt) { this.processingStartedAt = processingStartedAt; return this; }
        public Builder lastHeartbeatAt(Instant lastHeartbeatAt) { this.lastHeartbeatAt = lastHeartbeatAt; return this; }

        public SubtaskRecord build() {
            return new SubtaskRecord(this);
        }
    }
}
