package dsp1;

import software.amazon.awssdk.regions.Region;

public final class RuntimeConfig {
    public static final String PROP_REGION = "dsp.aws.region";
    public static final String PROP_BUCKET = "dsp.aws.bucket";
    public static final String PROP_AMI = "dsp.aws.ami";
    public static final String PROP_KEY_PAIR = "dsp.aws.keyPair";
    public static final String PROP_IAM_PROFILE = "dsp.aws.iamProfile";
    public static final String PROP_WORKER_LIMIT = "dsp.worker.limit";

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
}
