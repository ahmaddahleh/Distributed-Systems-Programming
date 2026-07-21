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
