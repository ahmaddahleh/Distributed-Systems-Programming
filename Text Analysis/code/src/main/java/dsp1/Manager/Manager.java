package dsp1.Manager;

import dsp1.AWS;
import org.json.JSONObject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {

    /* ============================ CONFIG & STATE ============================ */

    private static final int THREAD_POOL_SIZE = 5;

    // Queue names
    private static final String Q_LOCAL_TO_MANAGER = "LocalToManagerQueue";
    private static final String Q_MANAGER_TO_LOCAL = "ManagerToLocalQueue";
    private static final String Q_WORKERS_TO_MANAGER = "WorkersToManagerQueue";
    private static final String Q_MANAGER_TO_WORKERS = "ManagerToWorkersQueue";

    // Resolved URLs
    private static String urlLocalToManager;
    private static String urlManagerToLocal;
    private static String urlWorkersToManager;
    private static String urlManagerToWorkers;

    // AWS helper
    private static final AWS aws = AWS.getInstance();

    // Per-job state remains in memory; restart recovery is still a known limitation.
    private static final ConcurrentHashMap<String, JobState> jobs = new ConcurrentHashMap<>();

    // Termination flag (triggered when a job/args requests terminate)
    private static volatile boolean shutdownRequested = false;

    // Statistics
    private static AtomicInteger jobsSubmitted = new AtomicInteger(0);
    private static AtomicInteger jobsFinished = new AtomicInteger(0);

    private static ExecutorService executor;

    /* ============================ QUEUE HELPERS ============================ */

    private static void ensureQueueExists(String queueName) {
        aws.getSqs().createQueue(
                CreateQueueRequest.builder()
                        .queueName(queueName)
                        .build());
        System.out.println("Ensured queue exists: " + queueName);
    }

    private static String resolveQueueUrl(String queueName) {
        return aws.getSqs()
                .getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build())
                .queueUrl();
    }

    private static Message receiveSingleMessage(String queueUrl) {
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(10)
                .build();

        List<Message> msgs = aws.getSqs().receiveMessage(req).messages();
        return (msgs == null || msgs.isEmpty()) ? null : msgs.get(0);
    }

    private static void deleteMessage(String queueUrl, Message msg) {
        aws.getSqs().deleteMessage(
                DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(msg.receiptHandle())
                        .build());
    }

    private static void sendToWorkers(String body) {
        aws.getSqs().sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(urlManagerToWorkers)
                        .messageBody(body)
                        .build());
    }

    private static void sendToLocal(String body) {
        aws.getSqs().sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(urlManagerToLocal)
                        .messageBody(body)
                        .build());
    }

    /* ============================ S3 HELPERS ============================ */

    private static String downloadFromS3ToLocal(String s3Key) {
        String localName = "downloaded_" + s3Key.replace("/", "_");

        try {
            Files.deleteIfExists(Paths.get(localName));

            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(aws.bucketName)
                    .key(s3Key)
                    .build();

            aws.getS3().getObject(req, Paths.get(localName));
            System.out.println("Downloaded input file from S3: " + s3Key);
            return localName;

        } catch (Exception e) {
            System.err.println("Failed downloading S3 object: " + s3Key);
            throw new RuntimeException(e);
        }
    }

    private static void uploadHtmlToS3(String bucket, String key, String html) {
        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("text/html")
                .build();

        aws.getS3().putObject(put, RequestBody.fromString(html));
    }

    /*
     * ============================ WORKER MANAGEMENT ============================
     */

    private static int countRunningWorkers() {
        Filter roleFilter = Filter.builder()
                .name("tag:Role")
                .values("Worker")
                .build();

        DescribeInstancesResponse resp = aws.getEc2()
                .describeInstances(DescribeInstancesRequest.builder()
                        .filters(roleFilter)
                        .build());

        int count = 0;
        for (Reservation r : resp.reservations()) {
            for (Instance inst : r.instances()) {
                if ("running".equalsIgnoreCase(inst.state().nameAsString())) {
                    count++;
                }
            }
        }
        return count;
    }

    private static List<String> collectInstanceIdsByRole(String roleValue) {
        List<String> ids = new ArrayList<>();

        Filter filter = Filter.builder()
                .name("tag:Role")
                .values(roleValue)
                .build();

        DescribeInstancesResponse resp = aws.getEc2().describeInstances(DescribeInstancesRequest.builder()
                .filters(filter)
                .build());

        for (Reservation r : resp.reservations()) {
            for (Instance inst : r.instances()) {
                ids.add(inst.instanceId());
            }
        }

        return ids;
    }

    private static void spawnWorkerInstances(int count) {
        if (count <= 0)
            return;

        // *** script for workers ***
        String workerScript = "#!/bin/bash\n"
                + "yum update -y\n"
                + "yum install java-17-amazon-corretto -y\n"
                + "mkdir -p /home/ec2-user/app\n"
                + "aws s3 cp s3://" + aws.bucketName + "/system.jar /home/ec2-user/app/system.jar\n"
                + "nohup java -cp /home/ec2-user/app/system.jar dsp1.Worker.Worker > /home/ec2-user/app/worker.log 2>&1 &";

        for (int i = 0; i < count; i++) {
            aws.createEC2(workerScript, "Worker", 1);
        }

        System.out.println("Launched " + count + " worker instances.");
    }

    private static int freeWorkerSlots() {
        int maxWorkers = 7; // original limit logic preserved ---> after reading in aws rules
        int currentlyRunning = countRunningWorkers();
        return maxWorkers - currentlyRunning;
    }

    private static void ensureEnoughWorkers(List<JSONObject> tasks, int tasksPerWorker) {

        int running = countRunningWorkers();
        int required = (int) Math.ceil((double) tasks.size() / tasksPerWorker);

        int toLaunch = required - running;
        if (toLaunch <= 0) {
            System.out.println("Existing workers are enough. Running=" + running + ", required=" + required);
            return;
        }

        int available = freeWorkerSlots();
        if (available <= 0) {
            System.out.println("Maximum workers reached, cannot start more.");
            return;
        }

        int launchCount = Math.min(toLaunch, available);
        System.out
                .println("Launching " + launchCount + " workers (required " + required + ", running " + running + ").");

        spawnWorkerInstances(launchCount);
    }

    private static void dispatchTasksToWorkers(List<JSONObject> tasks) {
        for (JSONObject t : tasks) {
            sendToWorkers(t.toString());
        }
        System.out.println("Dispatched " + tasks.size() + " tasks to workers.");
    }

    /* ============================ INPUT PARSING ============================ */

    static List<JSONObject> buildWorkerTasksFromFile(String fileName, String taskId) {
        List<JSONObject> tasks = new ArrayList<>();

        try {
            List<WorkerTask> parsedTasks = buildWorkerTaskModelsFromFile(fileName, taskId);
            for (WorkerTask parsedTask : parsedTasks) {
                tasks.add(parsedTask.toJson());
            }

        } catch (Exception e) {
            System.err.println("Error reading input file: " + e.getMessage());
        }

        return tasks;
    }

    static List<WorkerTask> buildWorkerTaskModelsFromFile(String fileName, String taskId) {
        try {
            return InputTaskParser.parse(Files.readString(Paths.get(fileName)), taskId);
        } catch (Exception e) {
            System.err.println("Error reading input file: " + e.getMessage());
            return List.of();
        }
    }

    /* ============================ JOB HANDLING ============================ */

    private static void handleNewJobFromLocal(Message msg) {
        JSONObject obj = new JSONObject(msg.body());
        String bucket = obj.getString("s3Bucket");
        String taskId = obj.getString("taskId");

        // If termination already requested → reject new jobs
        if (shutdownRequested) {
            JSONObject blocked = new JSONObject();
            blocked.put("type", "blocked");
            blocked.put("description",
                    "Unable to receive task %s\nManager will be terminating soon..."
                            .formatted(taskId));
            blocked.put("taskId", taskId);
            sendToLocal(blocked.toString());
            deleteMessage(urlLocalToManager, msg);
            return;
        }

        String key = obj.getString("key");
        int workersRatio = obj.getInt("workers");

        // terminate flag from the local app (boolean in JSON, read as string)
        String terminateFlag = obj.optString("terminate");
        if ("true".equals(terminateFlag)) {
            shutdownRequested = true;
        }

        System.out.println("Received new job:");
        System.out.println("  taskId = " + taskId);
        System.out.println("  bucket = " + bucket);
        System.out.println("  key    = " + key);

        // Download the input file
        String localFile = downloadFromS3ToLocal(key);

        // Parse into worker tasks
        List<WorkerTask> parsedTasks = buildWorkerTaskModelsFromFile(localFile, taskId);
        jobs.put(taskId, JobState.fromTasks(taskId, parsedTasks));
        List<JSONObject> tasks = new ArrayList<>();
        for (WorkerTask parsedTask : parsedTasks) {
            tasks.add(parsedTask.toJson());
        }

        System.out.println("Parsed " + tasks.size() + " items from input file.");

        jobsSubmitted.incrementAndGet();

        if (tasks.isEmpty()) {
            checkIfJobCompleted(taskId);
            return;
        }

        // Adjust workers & dispatch tasks
        ensureEnoughWorkers(tasks, workersRatio);
        dispatchTasksToWorkers(tasks);
    }

    private static void handleTerminateFromLocal(Message msg) {
        System.out.println("Terminate message received from LocalApplication.");
        System.exit(0);
    }

    
    private static void accumulateWorkerSuccess(Message msg) {
        JSONObject obj = new JSONObject(msg.body());
        String taskId = obj.getString("taskId");
        String subTaskId = obj.optString("subTaskId", taskId);
        String resultLocation = obj.getString("result");
        String url = obj.getString("url");
        String analysis = obj.getString("analysis");

        acceptWorkerResult(new WorkerResultRecord(taskId, subTaskId, analysis, url, resultLocation, true));

        checkIfJobCompleted(taskId);
    }

    private static void accumulateWorkerFailure(Message msg) {
        JSONObject obj = new JSONObject(msg.body());
        String taskId = obj.getString("taskId");
        String subTaskId = obj.optString("subTaskId", taskId);
        String error = obj.getString("error");
        String url = obj.getString("url");
        String analysis = obj.getString("analysis");

        System.out.println("Worker reported failed subtask for task " + taskId
                + " | analysis=" + analysis
                + " | url=" + url
                + " | error=" + error);

        acceptWorkerResult(new WorkerResultRecord(taskId, subTaskId, analysis, url, "ERROR: " + error, false));

        checkIfJobCompleted(taskId);
    }

    static boolean acceptWorkerResult(WorkerResultRecord record) {
        JobState state = jobs.get(record.taskId());
        if (state == null) {
            System.out.println("Ignoring worker result for unknown job " + record.taskId());
            return false;
        }
        boolean accepted = state.acceptTerminalResult(record);
        if (!accepted) {
            System.out.println("Ignoring duplicate, conflicting, or unexpected worker result for taskId="
                    + record.taskId() + ", subTaskId=" + record.subTaskId());
        }
        return accepted;
    }

    static void clearJobsForTest() {
        jobs.clear();
    }

    static void putJobForTest(JobState state) {
        jobs.put(state.taskId(), state);
    }

    private static void checkIfJobCompleted(String taskId) {
        JobState state = jobs.get(taskId);
        if (state == null) {
            System.out.println("Ignoring completion check for unknown job " + taskId);
            return;
        }

        if (!state.isComplete() || !state.markFinalizing()) {
            return;
        }

        System.out.println("All sub-tasks for job " + taskId + " completed. Building summary...");

        List<String[]> results = state.summaryRows();
        String summaryKey = "results/" + taskId + "_summary.html";

        createSummaryHtmlAndUpload(taskId, results, summaryKey);

        JSONObject doneMsg = new JSONObject();
        doneMsg.put("type", "jobDone");
        doneMsg.put("taskId", taskId);
        doneMsg.put("s3Bucket", aws.bucketName);
        doneMsg.put("outputS3Key", summaryKey);

        sendToLocal(doneMsg.toString());
        System.out.println("Sent jobDone notification to LocalApplication for task " + taskId);

        jobs.remove(taskId);

        jobsFinished.incrementAndGet();
    }

    /* ============================ SUMMARY HTML ============================ */

    private static void createSummaryHtmlAndUpload(String taskId,
            List<String[]> entries,
            String summaryKey) {

        StringBuilder html = new StringBuilder();

        html.append("<html><head><title>Task ")
                .append(taskId)
                .append(" - Analysis Summary</title>")
                .append("<style>")
                .append("body{font-family:Arial,sans-serif;margin:25px;background:#f9f9f9;}")
                .append("h1{color:#333;border-bottom:2px solid #007bff;padding-bottom:8px;}")
                .append("table{width:100%;border-collapse:collapse;margin-top:20px;}")
                .append("th,td{border:1px solid #ddd;padding:8px;text-align:left;}")
                .append("th{background:#007bff;color:white;}")
                .append(".error{background:#fdd;color:#900;font-weight:bold;}")
                .append("</style></head><body>");

        html.append("<h1>Results for Task: ").append(taskId).append("</h1>");
        html.append("<table>");
        html.append("<tr><th>Analysis Type</th><th>Input File</th><th>Output / Error</th></tr>");

        for (String[] row : entries) {
            String offset = row.length == 4 ? row[0] : "";
            String analysisType = row.length == 4 ? row[1] : row[0];
            String inputUrl = row.length == 4 ? row[2] : row[1];
            String outputField = row.length == 4 ? row[3] : row[2];

            html.append("<tr>");
            html.append("<td>").append(analysisType).append("</td>");
            html.append("<td><a href='")
                    .append(inputUrl)
                    .append("' target='_blank'>Source</a></td>");

            try {
                if (outputField != null && outputField.startsWith("ERROR:")) {
                    html.append("<td class='error'>").append(outputField).append("</td>");
                } else {
                    String encodedKey = URLEncoder.encode(outputField, StandardCharsets.UTF_8.toString());
                    String publicLink = "https://" + aws.bucketName
                            + ".s3.us-east-1.amazonaws.com/" + encodedKey;

                    html.append("<td><a href='")
                            .append(publicLink)
                            .append("' target='_blank'>View Output</a></td>");
                }
            } catch (Exception e) {
                html.append("<td class='error'>UNEXPECTED ERROR: ")
                        .append(e.getMessage())
                        .append("</td>");
            }

            html.append("</tr>");
        }

        html.append("</table></body></html>");

        uploadHtmlToS3(aws.bucketName, summaryKey, html.toString());
        System.out.println("Summary file uploaded to S3 key: " + summaryKey);
    }

    /* ============================ TERMINATION ============================ */

    private static void terminateAll() {
        System.out.println("Terminating system...");

        // Terminate workers
        List<String> workerIds = collectInstanceIdsByRole("Worker");
        for (String id : workerIds) {
            terminateInstance(id);
        }

        System.out.println("All worker instances terminated by Manager.");

        // Terminate manager itself
        List<String> managerIds = collectInstanceIdsByRole("Manager");
        if (!managerIds.isEmpty()) {
            terminateInstance(managerIds.get(0));
        }

        System.out.println("Manager exiting.");
        System.exit(0);
    }

    private static void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest req = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            aws.getEc2().terminateInstances(req);
            System.out.println("Terminated instance: " + instanceId);
        } catch (Exception e) {
            System.err.printf("Failed to terminate instance %s: %s%n", instanceId, e.getMessage());
        }
    }

    /* ============================ DISPATCH HELPERS ============================ */

    private static void handleIncomingLocal(Message msg) {
        try {
            JSONObject obj = new JSONObject(msg.body());
            String type = obj.getString("type");
            System.out.println("Message from Local: " + msg.body());

            if ("newTask".equals(type)) {
                handleNewJobFromLocal(msg);
            } else if ("terminate".equals(type)) {
                handleTerminateFromLocal(msg);
            }

            // always delete after processing
            deleteMessage(urlLocalToManager, msg);

        } catch (Exception e) {
            System.err.println("Error handling Local message: " + e.getMessage());
        }
    }

    private static void handleIncomingWorker(Message msg) {
        System.out.println("Message from Worker: " + msg.body());
        ManagerWorkerResultAcknowledger.handle(
                msg,
                Manager::processIncomingWorkerResult,
                message -> deleteMessage(urlWorkersToManager, message));
    }

    private static void processIncomingWorkerResult(Message msg) {
        JSONObject obj = new JSONObject(msg.body());
        String type = obj.getString("type");

        // process based on type
        if ("jobDone".equals(type)) {
            accumulateWorkerSuccess(msg);
        } else if ("failedjob".equals(type)) {
            accumulateWorkerFailure(msg);
        }
    }

    /* ============================ MAIN LOOP ============================ */

    public static void main(String[] args) {
        // Initialize thread pool , used for presrving scalability as we was asked to implement
        executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Create queues and resolve URLs
        ensureQueueExists(Q_LOCAL_TO_MANAGER);
        ensureQueueExists(Q_MANAGER_TO_LOCAL);
        ensureQueueExists(Q_WORKERS_TO_MANAGER);
        ensureQueueExists(Q_MANAGER_TO_WORKERS);

        urlLocalToManager = resolveQueueUrl(Q_LOCAL_TO_MANAGER);
        urlManagerToLocal = resolveQueueUrl(Q_MANAGER_TO_LOCAL);
        urlWorkersToManager = resolveQueueUrl(Q_WORKERS_TO_MANAGER);
        urlManagerToWorkers = resolveQueueUrl(Q_MANAGER_TO_WORKERS);

        System.out.println("Manager started. Listening for messages...");

        while (true) {

            // 1) Pull from local applications
            Message fromLocal = receiveSingleMessage(urlLocalToManager);
            if (fromLocal != null) {
                executor.submit(() -> handleIncomingLocal(fromLocal));
            }

            // 2) Pull from workers
            Message fromWorker = receiveSingleMessage(urlWorkersToManager);
            if (fromWorker != null) {
                executor.submit(() -> handleIncomingWorker(fromWorker));
            }

            // 3) If all jobs finished and terminate was requested -> tear down
            if (jobsSubmitted.get() == jobsFinished.get() && jobsSubmitted.get() > 0) {
                System.out.println("All jobs processed.");
                if (shutdownRequested) {
                    terminateAll();
                } else {
                    // reset counts to allow new jobs
                    jobsSubmitted = new AtomicInteger(0);
                    jobsFinished = new AtomicInteger(0);
                }
            }
        }
    }
}
