package dsp1.Worker;

import dsp1.AWS;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Worker {

    /* ============================ CONFIG & STATE ============================ */

    private static final AWS aws = AWS.getInstance();

    // Queue names
    private static final String QUEUE_WORKER_TO_MANAGER = "WorkersToManagerQueue";
    private static final String QUEUE_MANAGER_TO_WORKER = "ManagerToWorkersQueue";

    // Resolved URLs
    private static String urlWorkerToManager;
    private static String urlManagerToWorker;

    private static String activeTaskId;
    private static String activeSubTaskId;
    private static String lastError = "";

    private static Message procMessage;

    /* ============================ MAIN LOOP ============================ */

    public static void main(String[] args) {

        // Resolve queue URLs
        urlManagerToWorker = aws.getSqs()
                .getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(QUEUE_MANAGER_TO_WORKER)
                        .build())
                .queueUrl();

        urlWorkerToManager = aws.getSqs()
                .getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(QUEUE_WORKER_TO_MANAGER)
                        .build())
                .queueUrl();

        while (true) {
            System.out.println("Worker awaiting tasks from Manager...");

            Message taskMsg = fetchMessage(urlManagerToWorker);
            // check if no message received
            if (taskMsg == null) {
                continue;
            }

            procMessage = taskMsg;
            removeMessage(QUEUE_MANAGER_TO_WORKER, taskMsg);

            System.out.println("Worker received SQS message: " + taskMsg.body());

            try {
                processTaskMessage(taskMsg);
            } catch (Exception e) {
                System.err.println("Unexpected error while processing task: " + e.getMessage());
            }
        }
    }

    /* ============================ TASK PROCESSING ============================ */

    private static void processTaskMessage(Message msg) {

        JSONObject payload = new JSONObject(msg.body());
        activeTaskId = payload.getString("taskId");
        activeSubTaskId = payload.optString("subTaskId", activeTaskId);
        String sourceUrl = payload.getString("url");
        String analysisType = payload.getString("analysis");

        lastError = "";

        // 1) Download input file
        File inputFile = downloadRemoteTextFile(sourceUrl, activeSubTaskId, "./");
        if (hasError()) {
            sendFailureToManager(sourceUrl, analysisType);
            return;
        }

        // 2) Analyze
        File resultFile = runAnalysis(inputFile, analysisType, activeSubTaskId);
        if (hasError() || resultFile == null) {
            sendFailureToManager(sourceUrl, analysisType);
            return;
        }

        // 3) Upload result to S3 and notify manager
        System.out.println("Analysis completed, uploading result to S3...");
    
        String keyName = ResultKeyBuilder.build(activeTaskId, activeSubTaskId, sourceUrl, analysisType);


        uploadResultToS3(resultFile, keyName);

        sendSuccessToManager(sourceUrl, analysisType, keyName);
    }

    /* ============================ SQS HELPERS ============================ */

    private static Message fetchMessage(String queueUrl) {
        ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(10)
                .build();

        List<Message> msgs = aws.getSqs().receiveMessage(req).messages();
        if (msgs == null || msgs.isEmpty()) {
            return null;
        }
        return msgs.get(0);
    }

    private static void removeMessage(String queueUrl, Message msg) {
        try {
            aws.getSqs().deleteMessage(
                    DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build());
            System.out.println("SQS message deleted successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete message: " + e.getMessage());
        }
    }

    private static void sendToManager(String body) {
        try {
            aws.getSqs().sendMessage(builder -> builder
                    .queueUrl(urlWorkerToManager)
                    .messageBody(body));
        } catch (Exception e) {
            lastError = "Failed to send message to manager: " + e.getMessage();
            System.err.println(lastError);
        }
    }

    private static void sendFailureToManager(String url, String analysis) {
        JSONObject fail = new JSONObject()
                .put("taskId", activeTaskId)
                .put("subTaskId", activeSubTaskId)
                .put("type", "failedjob")
                .put("error", lastError)
                .put("url", url)
                .put("analysis", analysis);

        sendToManager(fail.toString());
    }

    private static void sendSuccessToManager(String url, String analysis, String s3Key) {
        JSONObject success = new JSONObject()
                .put("taskId", activeTaskId)
                .put("subTaskId", activeSubTaskId)
                .put("type", "jobDone")
                .put("result", s3Key)
                .put("url", url)
                .put("analysis", analysis);

        sendToManager(success.toString());
    }

    /*
     * ============================ S3 & FILE HELPERS ============================
     */

    private static void uploadResultToS3(File file, String keyName) {
        try {
            aws.getS3().putObject(
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket("dsp-ahmad-dah") //hardcoded bucket name , you can change if needed
                            .key(keyName)
                            .build(),
                    file.toPath());
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage());
        }
    }

    private static File downloadRemoteTextFile(String url,
            String taskId,
            String destDir) {
        try {
            String outPath = destDir + "/" + ResultKeyBuilder.localInputFileName(taskId);
            InputStream in = new URL(url).openStream();
            Files.copy(in, Paths.get(outPath), StandardCopyOption.REPLACE_EXISTING);

            return new File(outPath);

        } catch (Exception e) {
            lastError = "Failed to download file from URL: " + e.getMessage();
            System.err.println(lastError);
            return null;
        }
    }

    /* ============================ NLP ANALYSIS ============================ */

    // Run analysis on input file and produce output file , According to analysis type which is more efficient

    private static File runAnalysis(File inputFile,
                               String analysisType,
                               String taskId) {

    StanfordAnalyzer nlp = StanfordAnalyzer.getInstance();
    File outputFile = new File("/tmp/" + ResultKeyBuilder.localOutputFileName(taskId));

    try {
        List<String> lines = Files.readAllLines(inputFile.toPath());
        StringBuilder sb = new StringBuilder();

        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }

            String analysisResult;

            switch (analysisType) {
                case "POS":
                    analysisResult = nlp.analyzePOS(line);
                    break;
                case "CONSTITUENCY":
                    analysisResult = nlp.analyzeConstituency(line);
                    break;
                case "DEPENDENCY":
                    analysisResult = nlp.analyzeDependency(line);
                    break;
                default:
                    analysisResult = "Unknown analysis type: " + analysisType;
            }

            sb.append("INPUT: ").append(line).append("\n");
            sb.append("OUTPUT: ").append(analysisResult).append("\n\n");
        }

        Files.writeString(outputFile.toPath(), sb.toString());
        return outputFile;

    } catch (Exception e) {
        lastError = "Failed to analyze file: ";
        System.err.println(lastError + e.getMessage());
        return null;
    }
}


    /* ============================ UTILS ============================ */

    private static boolean hasError() {
        return lastError != null && !lastError.isEmpty();
    }
}
