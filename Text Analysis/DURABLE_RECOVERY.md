# Durable Manager Recovery

This branch moves Manager progress tracking from process memory into DynamoDB-inspired durable state.

## DynamoDB Table

Default table name:

```text
DistributedTextAnalysisState
```

Override with `dsp.dynamodb.table` or `DSP_DYNAMODB_TABLE`.

Primary key:

```text
PK = JOB#<taskId>
SK = META
SK = SUBTASK#<subTaskId>
```

The application does not create or delete this table automatically.

## Job Item

The job row stores:

- `taskId`
- `inputBucket`
- `inputKey`
- `outputBucket`
- `outputFileName`
- `expectedSubtaskCount`
- `completedSubtaskCount`
- `inputParsingComplete`
- `status`
- `finalReportKey`
- `notificationStatus`
- `createdAt`
- `updatedAt`
- `version`
- `leaseOwner`
- `leaseExpiresAt`
- `failureReason`
- `terminate`

Job states:

```text
RECEIVED -> DISPATCHING -> RUNNING -> FINALIZING -> COMPLETED
RECEIVED/DISPATCHING/RUNNING/FINALIZING -> FAILED
```

## Subtask Item

Each valid input line gets a deterministic `subTaskId`:

```text
taskId + ":" + validTaskIndex
```

Subtask fields:

- `taskId`
- `subTaskId`
- `analysis`
- `url`
- `status`
- `resultS3Key`
- `errorMessage`
- `attemptCount`
- `createdAt`
- `updatedAt`
- `dispatchedAt`

Subtask states:

```text
PENDING -> DISPATCHED -> SUCCEEDED
PENDING -> DISPATCHED -> FAILED
```

Terminal states never change. First terminal result wins.

## Acknowledgement Ordering

Local job request messages are deleted only after the job row is created or the store confirms the same `taskId` already exists.

Worker result messages are deleted only after durable terminal result handling succeeds or the store confirms the message is a harmless duplicate/conflict.

If persistence fails, the SQS message remains visible after its timeout and can be retried.

## Recovery Algorithm

On startup and periodically, Manager:

1. Lists recoverable jobs.
2. Resumes jobs in `RECEIVED`, `DISPATCHING`, `RUNNING`, and stale `FINALIZING`.
3. Parses input if `inputParsingComplete=false`.
4. Persists missing subtasks idempotently.
5. Dispatches `PENDING` subtasks.
6. Redispatches stale `DISPATCHED` subtasks.
7. Finalizes jobs where all subtasks are terminal.
8. Retries completion notifications where `notificationStatus=PENDING`.

## Lease Behavior

Configurable settings:

- `DSP_MANAGER_LEASE_SECONDS`
- `DSP_MANAGER_STALE_DISPATCH_SECONDS`
- `DSP_MANAGER_RECOVERY_INTERVAL_SECONDS`
- `DSP_MANAGER_ID`

Only a Manager holding the finalization lease can mark a job `COMPLETED`.

Expired `FINALIZING` leases can be taken over. The report key is deterministic, so a repeated upload is safe:

```text
reports/<taskId>/summary.html
```

## Duplicate Handling

SQS can deliver duplicates and workers can process the same subtask more than once. The system does not claim exactly-once execution.

Application-level effects are idempotent:

- terminal subtask transition is conditional
- completed count increments only with the first terminal transition
- final report key is deterministic
- completion notification may duplicate, and LocalApplication filters by `taskId`

## Required IAM Permissions

For a real deployment, the Manager role needs permissions such as:

- `dynamodb:GetItem`
- `dynamodb:PutItem`
- `dynamodb:UpdateItem`
- `dynamodb:Query`
- `dynamodb:Scan`
- `dynamodb:TransactWriteItems`
- existing S3, SQS, and EC2 permissions used by the assignment

## Real Table Setup

Create a DynamoDB table manually or through IaC before running the app:

```text
TableName: DistributedTextAnalysisState
Partition key: PK (String)
Sort key: SK (String)
Billing mode: On-demand recommended for demos
```

This repository does not create the table automatically during tests.

## Remaining Limitations

- Real AWS integration was not run locally.
- SQS DLQ/redrive setup is still not automated.
- Exactly-once worker execution is impossible with SQS standard queues.
- A job can still be lost if the LocalApplication request disappears before the Manager durably creates the job; keeping the SQS message until persistence greatly narrows this window.
- EC2 autoscaling and production observability are outside this branch.

Running the distributed application may create AWS charges.
