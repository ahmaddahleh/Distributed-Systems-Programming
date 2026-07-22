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
    }

    @Test
    void usesAssignmentCompatibleDefaults() {
        assertEquals(Region.US_EAST_1, RuntimeConfig.awsRegion());
        assertEquals("dsp-ahmad-dah", RuntimeConfig.bucketName());
        assertEquals("ami-0c02fb55956c7d316", RuntimeConfig.amiId());
        assertEquals("vockey", RuntimeConfig.keyPairName());
        assertEquals("LabInstanceProfile", RuntimeConfig.iamInstanceProfile());
        assertEquals(7, RuntimeConfig.workerLimit());
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty(RuntimeConfig.PROP_REGION, "eu-west-1");
        System.setProperty(RuntimeConfig.PROP_BUCKET, "test-bucket");
        System.setProperty(RuntimeConfig.PROP_AMI, "ami-test");
        System.setProperty(RuntimeConfig.PROP_KEY_PAIR, "key-test");
        System.setProperty(RuntimeConfig.PROP_IAM_PROFILE, "profile-test");
        System.setProperty(RuntimeConfig.PROP_WORKER_LIMIT, "3");

        assertEquals(Region.EU_WEST_1, RuntimeConfig.awsRegion());
        assertEquals("test-bucket", RuntimeConfig.bucketName());
        assertEquals("ami-test", RuntimeConfig.amiId());
        assertEquals("key-test", RuntimeConfig.keyPairName());
        assertEquals("profile-test", RuntimeConfig.iamInstanceProfile());
        assertEquals(3, RuntimeConfig.workerLimit());
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
