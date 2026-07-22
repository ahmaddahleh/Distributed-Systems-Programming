package dsp1;

import software.amazon.awssdk.regions.Region;

public final class RuntimeConfig {
    public static final String PROP_REGION = "dsp.aws.region";
    public static final String PROP_BUCKET = "dsp.aws.bucket";
    public static final String PROP_AMI = "dsp.aws.ami";
    public static final String PROP_KEY_PAIR = "dsp.aws.keyPair";
    public static final String PROP_IAM_PROFILE = "dsp.aws.iamProfile";
    public static final String PROP_WORKER_LIMIT = "dsp.worker.limit";
    public static final String PROP_DYNAMODB_TABLE = "dsp.dynamodb.table";
    public static final String PROP_LEASE_DURATION_SECONDS = "dsp.manager.leaseSeconds";
    public static final String PROP_STALE_DISPATCH_SECONDS = "dsp.manager.staleDispatchSeconds";
    public static final String PROP_RECOVERY_INTERVAL_SECONDS = "dsp.manager.recoveryIntervalSeconds";
    public static final String PROP_MANAGER_ID = "dsp.manager.id";
    public static final String PROP_WORKER_ID = "dsp.worker.id";
    public static final String PROP_WORKER_PROCESSING_LEASE_SECONDS = "dsp.worker.processingLeaseSeconds";
    public static final String PROP_WORKER_LEASE_HEARTBEAT_SECONDS = "dsp.worker.leaseHeartbeatSeconds";
    public static final String PROP_WORKER_DUPLICATE_VISIBILITY_DELAY_SECONDS = "dsp.worker.duplicateVisibilityDelaySeconds";
    public static final String PROP_WORKER_SQS_VISIBILITY_EXTENSION_SECONDS = "dsp.worker.sqsVisibilityExtensionSeconds";

    private RuntimeConfig() {
    }

    public static Region awsRegion() {
        return Region.of(value(PROP_REGION, "DSP_AWS_REGION", "us-east-1"));
    }

    public static String bucketName() {
        return value(PROP_BUCKET, "DSP_AWS_BUCKET", "dsp-ahmad-dah");
    }

    public static String amiId() {
        return value(PROP_AMI, "DSP_AWS_AMI", "ami-0c02fb55956c7d316");
    }

    public static String keyPairName() {
        return value(PROP_KEY_PAIR, "DSP_AWS_KEY_PAIR", "vockey");
    }

    public static String iamInstanceProfile() {
        return value(PROP_IAM_PROFILE, "DSP_AWS_IAM_PROFILE", "LabInstanceProfile");
    }

    public static int workerLimit() {
        return Integer.parseInt(value(PROP_WORKER_LIMIT, "DSP_WORKER_LIMIT", "7"));
    }

    public static String dynamoDbTableName() {
        return value(PROP_DYNAMODB_TABLE, "DSP_DYNAMODB_TABLE", "DistributedTextAnalysisState");
    }

    public static long leaseDurationSeconds() {
        return Long.parseLong(value(PROP_LEASE_DURATION_SECONDS, "DSP_MANAGER_LEASE_SECONDS", "300"));
    }

    public static long staleDispatchSeconds() {
        return Long.parseLong(value(PROP_STALE_DISPATCH_SECONDS, "DSP_MANAGER_STALE_DISPATCH_SECONDS", "300"));
    }

    public static long recoveryIntervalSeconds() {
        return Long.parseLong(value(PROP_RECOVERY_INTERVAL_SECONDS, "DSP_MANAGER_RECOVERY_INTERVAL_SECONDS", "30"));
    }

    public static String managerId() {
        return value(PROP_MANAGER_ID, "DSP_MANAGER_ID", "manager-" + java.util.UUID.randomUUID());
    }

    public static String workerId() {
        return value(PROP_WORKER_ID, "DSP_WORKER_ID", "worker-" + java.util.UUID.randomUUID());
    }

    public static long workerProcessingLeaseSeconds() {
        long seconds = positiveLong(PROP_WORKER_PROCESSING_LEASE_SECONDS, "DSP_WORKER_PROCESSING_LEASE_SECONDS", 120);
        long heartbeatSeconds = workerLeaseHeartbeatSeconds();
        if (seconds <= heartbeatSeconds) {
            throw new IllegalArgumentException("Worker processing lease duration must be greater than heartbeat interval");
        }
        return seconds;
    }

    public static long workerLeaseHeartbeatSeconds() {
        return positiveLong(PROP_WORKER_LEASE_HEARTBEAT_SECONDS, "DSP_WORKER_LEASE_HEARTBEAT_SECONDS", 30);
    }

    public static long workerDuplicateVisibilityDelaySeconds() {
        return positiveLong(PROP_WORKER_DUPLICATE_VISIBILITY_DELAY_SECONDS,
                "DSP_WORKER_DUPLICATE_VISIBILITY_DELAY_SECONDS",
                30);
    }

    public static long workerSqsVisibilityExtensionSeconds() {
        return positiveLong(PROP_WORKER_SQS_VISIBILITY_EXTENSION_SECONDS,
                "DSP_WORKER_SQS_VISIBILITY_EXTENSION_SECONDS",
                120);
    }

    static String value(String propertyName, String envName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return defaultValue;
    }

    private static long positiveLong(String propertyName, String envName, long defaultValue) {
        long value = Long.parseLong(value(propertyName, envName, Long.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(propertyName + " must be greater than zero");
        }
        return value;
    }
}
