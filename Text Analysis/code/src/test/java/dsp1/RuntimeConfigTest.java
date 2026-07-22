package dsp1;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RuntimeConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(RuntimeConfig.PROP_REGION);
        System.clearProperty(RuntimeConfig.PROP_BUCKET);
        System.clearProperty(RuntimeConfig.PROP_AMI);
        System.clearProperty(RuntimeConfig.PROP_KEY_PAIR);
        System.clearProperty(RuntimeConfig.PROP_IAM_PROFILE);
        System.clearProperty(RuntimeConfig.PROP_WORKER_LIMIT);
        System.clearProperty(RuntimeConfig.PROP_DYNAMODB_TABLE);
        System.clearProperty(RuntimeConfig.PROP_LEASE_DURATION_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_STALE_DISPATCH_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_RECOVERY_INTERVAL_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_MANAGER_ID);
        System.clearProperty(RuntimeConfig.PROP_WORKER_ID);
        System.clearProperty(RuntimeConfig.PROP_WORKER_PROCESSING_LEASE_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_WORKER_LEASE_HEARTBEAT_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_WORKER_DUPLICATE_VISIBILITY_DELAY_SECONDS);
        System.clearProperty(RuntimeConfig.PROP_WORKER_SQS_VISIBILITY_EXTENSION_SECONDS);
    }

    @Test
    void usesAssignmentCompatibleDefaults() {
        assertEquals(Region.US_EAST_1, RuntimeConfig.awsRegion());
        assertEquals("dsp-ahmad-dah", RuntimeConfig.bucketName());
        assertEquals("ami-0c02fb55956c7d316", RuntimeConfig.amiId());
        assertEquals("vockey", RuntimeConfig.keyPairName());
        assertEquals("LabInstanceProfile", RuntimeConfig.iamInstanceProfile());
        assertEquals(7, RuntimeConfig.workerLimit());
        assertEquals("DistributedTextAnalysisState", RuntimeConfig.dynamoDbTableName());
        assertEquals(300, RuntimeConfig.leaseDurationSeconds());
        assertEquals(300, RuntimeConfig.staleDispatchSeconds());
        assertEquals(30, RuntimeConfig.recoveryIntervalSeconds());
        assertEquals(120, RuntimeConfig.workerProcessingLeaseSeconds());
        assertEquals(30, RuntimeConfig.workerLeaseHeartbeatSeconds());
        assertEquals(30, RuntimeConfig.workerDuplicateVisibilityDelaySeconds());
        assertEquals(120, RuntimeConfig.workerSqsVisibilityExtensionSeconds());
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty(RuntimeConfig.PROP_REGION, "eu-west-1");
        System.setProperty(RuntimeConfig.PROP_BUCKET, "test-bucket");
        System.setProperty(RuntimeConfig.PROP_AMI, "ami-test");
        System.setProperty(RuntimeConfig.PROP_KEY_PAIR, "key-test");
        System.setProperty(RuntimeConfig.PROP_IAM_PROFILE, "profile-test");
        System.setProperty(RuntimeConfig.PROP_WORKER_LIMIT, "3");
        System.setProperty(RuntimeConfig.PROP_DYNAMODB_TABLE, "table-test");
        System.setProperty(RuntimeConfig.PROP_LEASE_DURATION_SECONDS, "10");
        System.setProperty(RuntimeConfig.PROP_STALE_DISPATCH_SECONDS, "11");
        System.setProperty(RuntimeConfig.PROP_RECOVERY_INTERVAL_SECONDS, "12");
        System.setProperty(RuntimeConfig.PROP_MANAGER_ID, "manager-test");
        System.setProperty(RuntimeConfig.PROP_WORKER_ID, "worker-test");
        System.setProperty(RuntimeConfig.PROP_WORKER_PROCESSING_LEASE_SECONDS, "40");
        System.setProperty(RuntimeConfig.PROP_WORKER_LEASE_HEARTBEAT_SECONDS, "10");
        System.setProperty(RuntimeConfig.PROP_WORKER_DUPLICATE_VISIBILITY_DELAY_SECONDS, "13");
        System.setProperty(RuntimeConfig.PROP_WORKER_SQS_VISIBILITY_EXTENSION_SECONDS, "41");

        assertEquals(Region.EU_WEST_1, RuntimeConfig.awsRegion());
        assertEquals("test-bucket", RuntimeConfig.bucketName());
        assertEquals("ami-test", RuntimeConfig.amiId());
        assertEquals("key-test", RuntimeConfig.keyPairName());
        assertEquals("profile-test", RuntimeConfig.iamInstanceProfile());
        assertEquals(3, RuntimeConfig.workerLimit());
        assertEquals("table-test", RuntimeConfig.dynamoDbTableName());
        assertEquals(10, RuntimeConfig.leaseDurationSeconds());
        assertEquals(11, RuntimeConfig.staleDispatchSeconds());
        assertEquals(12, RuntimeConfig.recoveryIntervalSeconds());
        assertEquals("manager-test", RuntimeConfig.managerId());
        assertEquals("worker-test", RuntimeConfig.workerId());
        assertEquals(40, RuntimeConfig.workerProcessingLeaseSeconds());
        assertEquals(10, RuntimeConfig.workerLeaseHeartbeatSeconds());
        assertEquals(13, RuntimeConfig.workerDuplicateVisibilityDelaySeconds());
        assertEquals(41, RuntimeConfig.workerSqsVisibilityExtensionSeconds());
    }

    @Test
    void workerLeaseDurationMustExceedHeartbeatInterval() {
        System.setProperty(RuntimeConfig.PROP_WORKER_PROCESSING_LEASE_SECONDS, "10");
        System.setProperty(RuntimeConfig.PROP_WORKER_LEASE_HEARTBEAT_SECONDS, "10");

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                RuntimeConfig::workerProcessingLeaseSeconds);
    }

    @Test
    void userDataIsEncodedExactlyOnceByAwsLayer() {
        String script = "#!/bin/bash\necho hello";
        String encoded = UserDataEncoder.encode(script);

        assertEquals(script, new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8));
    }

    @Test
    void usEastOneBucketCreationDoesNotUseLocationConstraint() {
        System.setProperty(RuntimeConfig.PROP_REGION, "us-east-1");

        assertNull(AWS.createBucketRequest("bucket").createBucketConfiguration());
    }

    @Test
    void nonUsEastOneBucketCreationUsesRegionLocationConstraint() {
        System.setProperty(RuntimeConfig.PROP_REGION, "eu-west-1");

        assertEquals("eu-west-1", AWS.createBucketRequest("bucket")
                .createBucketConfiguration()
                .locationConstraintAsString());
    }
}
