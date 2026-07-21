package dsp1.persistence;

import java.time.Instant;

public final class JobRecord {
    private final String taskId;
    private final String inputBucket;
    private final String inputKey;
    private final String outputBucket;
    private final String outputFileName;
    private final int expectedSubtaskCount;
    private final int completedSubtaskCount;
    private final boolean inputParsingComplete;
    private final JobStatus status;
    private final String finalReportKey;
    private final NotificationStatus notificationStatus;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final long version;
    private final String leaseOwner;
    private final Instant leaseExpiresAt;
    private final String failureReason;
    private final boolean terminate;

    private JobRecord(Builder builder) {
        this.taskId = builder.taskId;
        this.inputBucket = builder.inputBucket;
        this.inputKey = builder.inputKey;
        this.outputBucket = builder.outputBucket;
        this.outputFileName = builder.outputFileName;
        this.expectedSubtaskCount = builder.expectedSubtaskCount;
        this.completedSubtaskCount = builder.completedSubtaskCount;
        this.inputParsingComplete = builder.inputParsingComplete;
        this.status = builder.status;
        this.finalReportKey = builder.finalReportKey;
        this.notificationStatus = builder.notificationStatus;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.version = builder.version;
        this.leaseOwner = builder.leaseOwner;
        this.leaseExpiresAt = builder.leaseExpiresAt;
        this.failureReason = builder.failureReason;
        this.terminate = builder.terminate;
    }

    public static Builder builder(String taskId) {
        return new Builder(taskId);
    }

    public Builder toBuilder() {
        return new Builder(taskId)
                .inputBucket(inputBucket)
                .inputKey(inputKey)
                .outputBucket(outputBucket)
                .outputFileName(outputFileName)
                .expectedSubtaskCount(expectedSubtaskCount)
                .completedSubtaskCount(completedSubtaskCount)
                .inputParsingComplete(inputParsingComplete)
                .status(status)
                .finalReportKey(finalReportKey)
                .notificationStatus(notificationStatus)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .version(version)
                .leaseOwner(leaseOwner)
                .leaseExpiresAt(leaseExpiresAt)
                .failureReason(failureReason)
                .terminate(terminate);
    }

    public String taskId() { return taskId; }
    public String inputBucket() { return inputBucket; }
    public String inputKey() { return inputKey; }
    public String outputBucket() { return outputBucket; }
    public String outputFileName() { return outputFileName; }
    public int expectedSubtaskCount() { return expectedSubtaskCount; }
    public int completedSubtaskCount() { return completedSubtaskCount; }
    public boolean inputParsingComplete() { return inputParsingComplete; }
    public JobStatus status() { return status; }
    public String finalReportKey() { return finalReportKey; }
    public NotificationStatus notificationStatus() { return notificationStatus; }
    public Instant createdAt() { return createdAt; }
    public Instant updatedAt() { return updatedAt; }
    public long version() { return version; }
    public String leaseOwner() { return leaseOwner; }
    public Instant leaseExpiresAt() { return leaseExpiresAt; }
    public String failureReason() { return failureReason; }
    public boolean terminate() { return terminate; }

    public boolean isRecoverable() {
        return status == JobStatus.RECEIVED
                || status == JobStatus.DISPATCHING
                || status == JobStatus.RUNNING
                || status == JobStatus.FINALIZING
                || (status == JobStatus.COMPLETED && notificationStatus == NotificationStatus.PENDING);
    }

    public static final class Builder {
        private final String taskId;
        private String inputBucket = "";
        private String inputKey = "";
        private String outputBucket = "";
        private String outputFileName = "";
        private int expectedSubtaskCount;
        private int completedSubtaskCount;
        private boolean inputParsingComplete;
        private JobStatus status = JobStatus.RECEIVED;
        private String finalReportKey = "";
        private NotificationStatus notificationStatus = NotificationStatus.PENDING;
        private Instant createdAt = Instant.EPOCH;
        private Instant updatedAt = Instant.EPOCH;
        private long version;
        private String leaseOwner = "";
        private Instant leaseExpiresAt = Instant.EPOCH;
        private String failureReason = "";
        private boolean terminate;

        private Builder(String taskId) {
            this.taskId = taskId;
        }

        public Builder inputBucket(String inputBucket) { this.inputBucket = inputBucket; return this; }
        public Builder inputKey(String inputKey) { this.inputKey = inputKey; return this; }
        public Builder outputBucket(String outputBucket) { this.outputBucket = outputBucket; return this; }
        public Builder outputFileName(String outputFileName) { this.outputFileName = outputFileName; return this; }
        public Builder expectedSubtaskCount(int expectedSubtaskCount) { this.expectedSubtaskCount = expectedSubtaskCount; return this; }
        public Builder completedSubtaskCount(int completedSubtaskCount) { this.completedSubtaskCount = completedSubtaskCount; return this; }
        public Builder inputParsingComplete(boolean inputParsingComplete) { this.inputParsingComplete = inputParsingComplete; return this; }
        public Builder status(JobStatus status) { this.status = status; return this; }
        public Builder finalReportKey(String finalReportKey) { this.finalReportKey = finalReportKey; return this; }
        public Builder notificationStatus(NotificationStatus notificationStatus) { this.notificationStatus = notificationStatus; return this; }
        public Builder createdAt(Instant createdAt) { this.createdAt = createdAt; return this; }
        public Builder updatedAt(Instant updatedAt) { this.updatedAt = updatedAt; return this; }
        public Builder version(long version) { this.version = version; return this; }
        public Builder leaseOwner(String leaseOwner) { this.leaseOwner = leaseOwner; return this; }
        public Builder leaseExpiresAt(Instant leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; return this; }
        public Builder failureReason(String failureReason) { this.failureReason = failureReason; return this; }
        public Builder terminate(boolean terminate) { this.terminate = terminate; return this; }

        public JobRecord build() {
            return new JobRecord(this);
        }
    }
}
