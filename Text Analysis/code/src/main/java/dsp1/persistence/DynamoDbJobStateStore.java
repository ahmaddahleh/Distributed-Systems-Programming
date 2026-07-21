package dsp1.persistence;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class DynamoDbJobStateStore implements JobStateStore {
    private static final String SK_META = "META";
    private final DynamoDbClient dynamoDb;
    private final String tableName;

    public DynamoDbJobStateStore(DynamoDbClient dynamoDb, String tableName) {
        this.dynamoDb = dynamoDb;
        this.tableName = tableName;
    }

    @Override
    public boolean createJobIfAbsent(JobRecord job) {
        try {
            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(jobItem(job))
                    .conditionExpression("attribute_not_exists(PK)")
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public Optional<JobRecord> loadJob(String taskId) {
        Map<String, AttributeValue> item = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(key(taskId, SK_META))
                .consistentRead(true)
                .build()).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(toJob(item));
    }

    @Override
    public void saveSubtasksIfAbsent(String taskId, List<SubtaskRecord> subtasks) {
        for (SubtaskRecord subtask : subtasks) {
            try {
                dynamoDb.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(subtaskItem(subtask))
                        .conditionExpression("attribute_not_exists(PK)")
                        .build());
            } catch (ConditionalCheckFailedException ignored) {
            }
        }
        updateJobStatus(taskId, JobStatus.DISPATCHING, subtasks.isEmpty() ? Instant.now() : subtasks.get(subtasks.size() - 1).updatedAt());
    }

    @Override
    public void markInputParsingComplete(String taskId, int expectedSubtaskCount, Instant now) {
        dynamoDb.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key(taskId, SK_META))
                .updateExpression("SET expectedSubtaskCount = :expected, inputParsingComplete = :true, #status = :running, updatedAt = :updated ADD version :one")
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(
                        ":expected", n(expectedSubtaskCount),
                        ":true", bool(true),
                        ":running", s(JobStatus.RUNNING.name()),
                        ":updated", s(now.toString()),
                        ":one", n(1)))
                .build());
    }

    @Override
    public List<SubtaskRecord> listSubtasks(String taskId) {
        List<SubtaskRecord> records = new ArrayList<>();
        dynamoDb.queryPaginator(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk AND begins_with(SK, :prefix)")
                .expressionAttributeValues(Map.of(
                        ":pk", s(pk(taskId)),
                        ":prefix", s("SUBTASK#")))
                .consistentRead(true)
                .build()).forEach(page -> page.items().forEach(item -> records.add(toSubtask(item))));
        records.sort(java.util.Comparator.comparing(SubtaskRecord::subTaskId));
        return records;
    }

    @Override
    public List<JobRecord> listRecoverableJobs(Instant now) {
        List<JobRecord> records = new ArrayList<>();
        dynamoDb.scanPaginator(ScanRequest.builder()
                .tableName(tableName)
                .filterExpression("SK = :meta")
                .expressionAttributeValues(Map.of(":meta", s(SK_META)))
                .build()).forEach(page -> page.items().forEach(item -> {
                    JobRecord job = toJob(item);
                    if (job.isRecoverable()) {
                        records.add(job);
                    }
                }));
        records.sort(java.util.Comparator.comparing(JobRecord::createdAt));
        return records;
    }

    @Override
    public boolean claimJobLease(String taskId, String managerId, Instant now, Duration leaseDuration) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(taskId, SK_META))
                    .conditionExpression("attribute_not_exists(leaseOwner) OR leaseOwner = :manager OR leaseExpiresAt < :now OR leaseOwner = :blank")
                    .updateExpression("SET leaseOwner = :manager, leaseExpiresAt = :expires, updatedAt = :updated ADD version :one")
                    .expressionAttributeValues(Map.of(
                            ":manager", s(managerId),
                            ":expires", s(now.plus(leaseDuration).toString()),
                            ":now", s(now.toString()),
                            ":blank", s(""),
                            ":updated", s(now.toString()),
                            ":one", n(1)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public boolean markDispatchAttempt(String taskId, String subTaskId, Instant now) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(taskId, subtaskSk(subTaskId)))
                    .conditionExpression("#status <> :succeeded AND #status <> :failed")
                    .updateExpression("SET #status = :dispatched, dispatchedAt = :now, updatedAt = :now ADD attemptCount :one")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":succeeded", s(SubtaskStatus.SUCCEEDED.name()),
                            ":failed", s(SubtaskStatus.FAILED.name()),
                            ":dispatched", s(SubtaskStatus.DISPATCHED.name()),
                            ":now", s(now.toString()),
                            ":one", n(1)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public TerminalResultStatus acceptTerminalResult(WorkerTerminalResult result, Instant now) {
        List<TransactWriteItem> writes = List.of(
                TransactWriteItem.builder().update(Update.builder()
                        .tableName(tableName)
                        .key(key(result.taskId(), subtaskSk(result.subTaskId())))
                        .conditionExpression("attribute_exists(PK) AND #status <> :succeeded AND #status <> :failed")
                        .updateExpression("SET #status = :terminal, resultS3Key = :result, errorMessage = :error, updatedAt = :now")
                        .expressionAttributeNames(Map.of("#status", "status"))
                        .expressionAttributeValues(Map.of(
                                ":succeeded", s(SubtaskStatus.SUCCEEDED.name()),
                                ":failed", s(SubtaskStatus.FAILED.name()),
                                ":terminal", s(result.success() ? SubtaskStatus.SUCCEEDED.name() : SubtaskStatus.FAILED.name()),
                                ":result", s(result.resultS3Key()),
                                ":error", s(result.errorMessage()),
                                ":now", s(now.toString())))
                        .build()).build(),
                TransactWriteItem.builder().update(Update.builder()
                        .tableName(tableName)
                        .key(key(result.taskId(), SK_META))
                        .conditionExpression("attribute_exists(PK)")
                        .updateExpression("SET updatedAt = :now ADD completedSubtaskCount :one, version :one")
                        .expressionAttributeValues(Map.of(":now", s(now.toString()), ":one", n(1)))
                        .build()).build());
        try {
            dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
            return TerminalResultStatus.ACCEPTED;
        } catch (TransactionCanceledException e) {
            Optional<SubtaskRecord> existing = listSubtasks(result.taskId()).stream()
                    .filter(s -> s.subTaskId().equals(result.subTaskId()))
                    .findFirst();
            if (existing.isEmpty() || loadJob(result.taskId()).isEmpty()) {
                return TerminalResultStatus.UNKNOWN_JOB_OR_SUBTASK;
            }
            return existing.get().status().isTerminal()
                    ? TerminalResultStatus.DUPLICATE_OR_CONFLICT
                    : TerminalResultStatus.UNKNOWN_JOB_OR_SUBTASK;
        }
    }

    @Override
    public boolean claimFinalization(String taskId, String managerId, Instant now, Duration leaseDuration) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(taskId, SK_META))
                    .conditionExpression("inputParsingComplete = :true AND ((#status = :running AND completedSubtaskCount = expectedSubtaskCount) OR (#status = :finalizing AND leaseExpiresAt < :now))")
                    .updateExpression("SET #status = :finalizing, leaseOwner = :manager, leaseExpiresAt = :expires, updatedAt = :updated ADD version :one")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":true", bool(true),
                            ":running", s(JobStatus.RUNNING.name()),
                            ":finalizing", s(JobStatus.FINALIZING.name()),
                            ":now", s(now.toString()),
                            ":manager", s(managerId),
                            ":expires", s(now.plus(leaseDuration).toString()),
                            ":updated", s(now.toString()),
                            ":one", n(1)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public boolean markJobCompleted(String taskId, String managerId, String finalReportKey, Instant now) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(taskId, SK_META))
                    .conditionExpression("#status = :finalizing AND leaseOwner = :manager")
                    .updateExpression("SET #status = :completed, finalReportKey = :report, notificationStatus = :pending, updatedAt = :updated ADD version :one")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":finalizing", s(JobStatus.FINALIZING.name()),
                            ":manager", s(managerId),
                            ":completed", s(JobStatus.COMPLETED.name()),
                            ":report", s(finalReportKey),
                            ":pending", s(NotificationStatus.PENDING.name()),
                            ":updated", s(now.toString()),
                            ":one", n(1)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public boolean markCompletionNotificationSent(String taskId, Instant now) {
        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key(taskId, SK_META))
                    .conditionExpression("#status = :completed")
                    .updateExpression("SET notificationStatus = :sent, updatedAt = :updated ADD version :one")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":completed", s(JobStatus.COMPLETED.name()),
                            ":sent", s(NotificationStatus.SENT.name()),
                            ":updated", s(now.toString()),
                            ":one", n(1)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public boolean failJob(String taskId, String failureReason, Instant now) {
        dynamoDb.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key(taskId, SK_META))
                .updateExpression("SET #status = :status, failureReason = :reason, updatedAt = :updated ADD version :one")
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(
                        ":status", s(JobStatus.FAILED.name()),
                        ":reason", s(failureReason),
                        ":updated", s(now.toString()),
                        ":one", n(1)))
                .build());
        return true;
    }

    private void updateJobStatus(String taskId, JobStatus status, Instant now) {
        updateJobStatus(taskId, status, now, Map.of());
    }

    private void updateJobStatus(String taskId, JobStatus status, Instant now, Map<String, AttributeValue> extraValues) {
        Map<String, AttributeValue> values = new HashMap<>(extraValues);
        values.put(":status", s(status.name()));
        values.put(":updated", s(now.toString()));
        values.put(":one", n(1));
        dynamoDb.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key(taskId, SK_META))
                .updateExpression("SET #status = :status, updatedAt = :updated ADD version :one")
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(values)
                .build());
    }

    private static Map<String, AttributeValue> jobItem(JobRecord job) {
        Map<String, AttributeValue> item = baseItem(job.taskId(), SK_META, "JOB");
        item.put("taskId", s(job.taskId()));
        item.put("inputBucket", s(job.inputBucket()));
        item.put("inputKey", s(job.inputKey()));
        item.put("outputBucket", s(job.outputBucket()));
        item.put("outputFileName", s(job.outputFileName()));
        item.put("expectedSubtaskCount", n(job.expectedSubtaskCount()));
        item.put("completedSubtaskCount", n(job.completedSubtaskCount()));
        item.put("inputParsingComplete", bool(job.inputParsingComplete()));
        item.put("status", s(job.status().name()));
        item.put("finalReportKey", s(job.finalReportKey()));
        item.put("notificationStatus", s(job.notificationStatus().name()));
        item.put("createdAt", s(job.createdAt().toString()));
        item.put("updatedAt", s(job.updatedAt().toString()));
        item.put("version", n(job.version()));
        item.put("leaseOwner", s(job.leaseOwner()));
        item.put("leaseExpiresAt", s(job.leaseExpiresAt().toString()));
        item.put("failureReason", s(job.failureReason()));
        item.put("terminate", bool(job.terminate()));
        return item;
    }

    private static Map<String, AttributeValue> subtaskItem(SubtaskRecord subtask) {
        Map<String, AttributeValue> item = baseItem(subtask.taskId(), subtaskSk(subtask.subTaskId()), "SUBTASK");
        item.put("taskId", s(subtask.taskId()));
        item.put("subTaskId", s(subtask.subTaskId()));
        item.put("analysis", s(subtask.analysis()));
        item.put("url", s(subtask.url()));
        item.put("status", s(subtask.status().name()));
        item.put("resultS3Key", s(subtask.resultS3Key()));
        item.put("errorMessage", s(subtask.errorMessage()));
        item.put("attemptCount", n(subtask.attemptCount()));
        item.put("createdAt", s(subtask.createdAt().toString()));
        item.put("updatedAt", s(subtask.updatedAt().toString()));
        item.put("dispatchedAt", s(subtask.dispatchedAt().toString()));
        return item;
    }

    private static JobRecord toJob(Map<String, AttributeValue> item) {
        return JobRecord.builder(str(item, "taskId"))
                .inputBucket(str(item, "inputBucket"))
                .inputKey(str(item, "inputKey"))
                .outputBucket(str(item, "outputBucket"))
                .outputFileName(str(item, "outputFileName"))
                .expectedSubtaskCount(integer(item, "expectedSubtaskCount"))
                .completedSubtaskCount(integer(item, "completedSubtaskCount"))
                .inputParsingComplete(bool(item, "inputParsingComplete"))
                .status(JobStatus.valueOf(str(item, "status")))
                .finalReportKey(str(item, "finalReportKey"))
                .notificationStatus(NotificationStatus.valueOf(str(item, "notificationStatus")))
                .createdAt(instant(item, "createdAt"))
                .updatedAt(instant(item, "updatedAt"))
                .version(longNumber(item, "version"))
                .leaseOwner(str(item, "leaseOwner"))
                .leaseExpiresAt(instant(item, "leaseExpiresAt"))
                .failureReason(str(item, "failureReason"))
                .terminate(bool(item, "terminate"))
                .build();
    }

    private static SubtaskRecord toSubtask(Map<String, AttributeValue> item) {
        return SubtaskRecord.builder(str(item, "taskId"), str(item, "subTaskId"))
                .analysis(str(item, "analysis"))
                .url(str(item, "url"))
                .status(SubtaskStatus.valueOf(str(item, "status")))
                .resultS3Key(str(item, "resultS3Key"))
                .errorMessage(str(item, "errorMessage"))
                .attemptCount(integer(item, "attemptCount"))
                .createdAt(instant(item, "createdAt"))
                .updatedAt(instant(item, "updatedAt"))
                .dispatchedAt(instant(item, "dispatchedAt"))
                .build();
    }

    private static Map<String, AttributeValue> baseItem(String taskId, String sk, String entityType) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK", s(pk(taskId)));
        item.put("SK", s(sk));
        item.put("entityType", s(entityType));
        return item;
    }

    private static Map<String, AttributeValue> key(String taskId, String sk) {
        return Map.of("PK", s(pk(taskId)), "SK", s(sk));
    }

    private static String pk(String taskId) {
        return "JOB#" + taskId;
    }

    private static String subtaskSk(String subTaskId) {
        return "SUBTASK#" + subTaskId;
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value == null ? "" : value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static AttributeValue bool(boolean value) {
        return AttributeValue.builder().bool(value).build();
    }

    private static String str(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value == null || value.s() == null ? "" : value.s();
    }

    private static int integer(Map<String, AttributeValue> item, String key) {
        return (int) longNumber(item, key);
    }

    private static long longNumber(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value == null || value.n() == null ? 0 : Long.parseLong(value.n());
    }

    private static boolean bool(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value != null && Boolean.TRUE.equals(value.bool());
    }

    private static Instant instant(Map<String, AttributeValue> item, String key) {
        String value = str(item, key);
        return value.isBlank() ? Instant.EPOCH : Instant.parse(value);
    }
}
