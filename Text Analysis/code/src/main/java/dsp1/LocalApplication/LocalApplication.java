package dsp1.LocalApplication;

import dsp1.AWS;
import org.json.JSONObject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.net.URL;
import java.util.*;

public class LocalApplication {

    /*
     * ------------------------------ CLI arguments ------------------------------
     */

    private static File localInputFile;
    private static String inputName;
    private static String outputName;
    private static int maxTasksPerWorker;
    private static boolean shouldTerminate;

    /* ------------------------------ AWS Clients ------------------------------ */

    private static final AWS aws = AWS.getInstance();
    private static final Region activeRegion = Region.US_EAST_1;

    private static final Ec2Client ec2 = Ec2Client.builder().region(activeRegion).build();
    private static final S3Client s3 = S3Client.builder().region(activeRegion).build();

    /*
     * ------------------------------ System constants
     * ------------------------------
     */

    private static final String ROLE_KEY = "Role";
    private static final String ROLE_MANAGER = "Manager";

    private static final String BUCKET = "dsp-ahmad-dah";

    private static final String QUEUE_MANAGER_TO_LOCAL = "ManagerToLocalQueue";
    private static final String QUEUE_LOCAL_TO_MANAGER = "LocalToManagerQueue";

    private static String queueUrlManagerToLocal;
    private static String queueUrlLocalToManager;

    private static String taskIdentifier = "";

    /* ======================================================================== */
    /* SQS HELPERS */
    /* ======================================================================== */

    private static String resolveQueueUrl(String qName) {
        return aws.getSqs().getQueueUrl(
                GetQueueUrlRequest.builder().queueName(qName).build()).queueUrl();
    }

    private static void sendTaskRequest(String bucket, String key, int ratio) {

        JSONObject payload = new JSONObject();
        payload.put("type", "newTask");
        payload.put("taskId", taskIdentifier);
        payload.put("s3Bucket", bucket);
        payload.put("key", key);
        payload.put("outputFile", outputName);
        payload.put("workers", ratio);
        payload.put("terminate", shouldTerminate);

        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrlLocalToManager)
                .messageBody(payload.toString())
                .build();

        System.out.println("Dispatching task message to Manager → " + payload);
        aws.getSqs().sendMessage(request);
    }

    private static Message pollSingleMessage(String url) {
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(2)
                .build();

        List<Message> msgs = aws.getSqs().receiveMessage(req).messages();
        return (msgs == null || msgs.isEmpty()) ? null : msgs.get(0);
    }

    private static void removeMessage(String queue, Message m) {
        aws.getSqs().deleteMessage(
                DeleteMessageRequest.builder()
                        .queueUrl(queue)
                        .receiptHandle(m.receiptHandle())
                        .build());
    }

    private static void restoreMessageVisibility(String queue, Message m) {
    aws.getSqs().changeMessageVisibility(
    ChangeMessageVisibilityRequest.builder()
    .queueUrl(queue)
    .receiptHandle(m.receiptHandle())
    .visibilityTimeout(0)
    .build()
    );
    }

    /* ======================================================================== */
    /* EC2 HELPERS */
    /* ======================================================================== */

    private static List<String> runningManagers() {
        Filter managerTag = Filter.builder()
                .name("tag:" + ROLE_KEY)
                .values(ROLE_MANAGER)
                .build();

        DescribeInstancesResponse resp = ec2.describeInstances(
                DescribeInstancesRequest.builder().filters(managerTag).build());

        List<String> active = new ArrayList<>();
        for (Reservation res : resp.reservations()) {
            for (Instance i : res.instances()) {
                if (i.state().name() == InstanceStateName.RUNNING) {
                    active.add(i.instanceId());
                }
            }
        }
        return active;
    }

    private static String ensureManagerRunning() {

        List<String> managers = runningManagers();

        if (!managers.isEmpty()) {
            System.out.println("Manager is active: " + managers.get(0));
            return managers.get(0);
        }

        System.out.println("No Manager found → launching fresh instance...");

        uploadManagerJar();

        String userData = """
                #!/bin/bash
                sudo yum update -y
                sudo yum install -y java-17-amazon-corretto
                mkdir -p /home/ec2-user/app
                aws s3 cp s3://%s/system.jar /home/ec2-user/app/system.jar
                nohup java -jar /home/ec2-user/app/system.jar > /home/ec2-user/app/log.txt 2>&1 &
                """.formatted(BUCKET);

        String encoded = Base64.getEncoder().encodeToString(userData.getBytes());
        String newManagerId = aws.createEC2(encoded, "Manager", 1);

        System.out.println("New Manager launched ID=" + newManagerId);
        return newManagerId;
    }

    /* ======================================================================== */
    /* S3 HELPERS */
    /* ======================================================================== */

    private static void verifyOrCreateBucket() {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(BUCKET).build());
        } catch (NoSuchBucketException e) {
            System.out.println("Bucket missing → creating: " + BUCKET);
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    private static void uploadManagerJar() {
        try {
            s3.headObject(
                    HeadObjectRequest.builder().bucket(BUCKET).key("system.jar").build());
            return;
        } catch (NoSuchKeyException ignored) {
        }

        File jar = new File("target/dsp1-1.0-SNAPSHOT.jar");

        if (!jar.exists()) {
            throw new RuntimeException("System JAR missing! Run mvn package.");
        }

        System.out.println("Uploading Manager JAR to S3...");
        s3.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key("system.jar").build(),
                RequestBody.fromFile(jar));
    }

    private static void pushInputFile(String bucket, String key) {
        if (!localInputFile.exists()) {
            throw new RuntimeException("Input file missing: " + inputName);
        }
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(localInputFile));
    }

    private static void fetchFromS3(String bucket, String key, String localOut) {
        try {
            s3.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    new File(localOut).toPath());
        } catch (Exception e) {
            throw new RuntimeException("Failed downloading summary HTML", e);
        }
    }

    /* ======================================================================== */
    /* MAIN */
    /* ======================================================================== */

    public static void main(String[] args) {

        /* ----------- Parse CLI arguments ----------- */
        if (args.length < 3) {
            System.err.println("Usage: LocalApplication <input> <output> <n> [terminate]");
            return;
        }

        localInputFile = loadResourceFile(args[0]);
        inputName = args[0];
        outputName = args[1];
        maxTasksPerWorker = Integer.parseInt(args[2]);
        shouldTerminate = (args.length == 4 && args[3].equals("terminate"));

        /* ----------- Environment preparation ----------- */
        verifyOrCreateBucket();
        ensureManagerRunning();

        queueUrlManagerToLocal = resolveQueueUrl(QUEUE_MANAGER_TO_LOCAL);
        queueUrlLocalToManager = resolveQueueUrl(QUEUE_LOCAL_TO_MANAGER);

        /* ----------- Generate a unique task ID ----------- */
        taskIdentifier = UUID.randomUUID().toString();
        String s3Key = inputName + taskIdentifier;

        System.out.println("Uploading input file with S3 key: " + s3Key);
        pushInputFile(BUCKET, s3Key);

        sendTaskRequest(BUCKET, s3Key, maxTasksPerWorker);

        /* ----------- Response loop ----------- */
        while (true) {

            Message m = pollSingleMessage(queueUrlManagerToLocal);

            if (m == null) {
                sleep(150);
                continue;
            }

            JSONObject json = new JSONObject(m.body());
            String incomingTaskId = json.getString("taskId");

            /* Message not for us → return visibility */
            if (!incomingTaskId.equals(taskIdentifier)) {
                restoreMessageVisibility(queueUrlManagerToLocal, m);
                continue;
            }

            System.out.println("Received Manager response: " + json);

            if (json.getString("type").equals("blocked")) {
                System.out.println(json.getString("description"));
                removeMessage(queueUrlManagerToLocal, m);
                System.exit(0);
            }

            String summaryKey = json.getString("outputS3Key");
            fetchFromS3(BUCKET, summaryKey, outputName);

            System.out.println("Summary saved → " + new File(outputName).getAbsolutePath());

            removeMessage(queueUrlManagerToLocal, m);
            System.exit(0);
        }
    }

    /* ---------------- Helper ---------------- */
    private static File loadResourceFile(String name) {
        try {
            URL r = LocalApplication.class.getClassLoader().getResource(name);
            return new File(r.toURI());
        } catch (Exception e) {
            throw new RuntimeException("Cannot load resource " + name, e);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ignored) {
        }
    }
}
