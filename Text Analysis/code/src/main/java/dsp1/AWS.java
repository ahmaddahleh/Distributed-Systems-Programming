package dsp1;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;

import java.util.Base64;

public class AWS {

    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-00e95a9222311e8ed";

    public static Region reigon = Region.US_EAST_1;

    public String bucketName = "dsp-ahmad-dah";

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(reigon).build();
        sqs = SqsClient.builder().region(reigon).build();
        ec2 = Ec2Client.builder().region(reigon).build();
    }

    public SqsClient getSqs() {
        return sqs;
    }
    

    public S3Client getS3() { return this.s3; }
    public Ec2Client getEc2() { return this.ec2; }
    
    public static AWS getInstance() {
    
        return instance;
    }

    

    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(bucketName)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(
                    HeadBucketRequest.builder()
                            .bucket(bucketName)
                            .build()
            );
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MEDIUM)
                .imageId("ami-0c02fb55956c7d316") 
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

       Tag tag = Tag.builder()
        .key("Role")
        .value(tagName)
        .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s%n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    // SQS
    public void createSqsQueue(String queueName) {
       
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }
}