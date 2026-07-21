# Reliability Notes

This branch focuses on interview-ready reliability fixes without a production redesign.

## Message acknowledgement

Worker task messages from `ManagerToWorkersQueue` are deleted only after:

1. The task message is parsed.
2. The worker finishes processing or builds a terminal failure result.
3. The worker sends `jobDone` or `failedjob` to `WorkersToManagerQueue`.
4. The terminal result send succeeds.

Manager result messages from `WorkersToManagerQueue` are deleted only after the Manager records the terminal result.

## Subtask identity

Each valid input line receives a deterministic `subTaskId`:

```text
taskId + ":" + validTaskIndex
```

Malformed and blank input lines are skipped. The index is dense over valid lines only, so two identical valid lines become different subtasks.

## Idempotency

The Manager tracks completion by `subTaskId`, not by raw result count or by `analysis + URL`.

Conflict policy:

```text
first accepted terminal result wins
```

Later duplicate or conflicting results for the same `subTaskId` are logged and ignored. This prevents duplicate SQS deliveries from finishing a job early or duplicating rows.

## Remaining limitations

Manager state is now represented through `JobStateStore`, with `DynamoDbJobStateStore` for real runs and `InMemoryJobStateStore` for tests.

The durable recovery path persists job and subtask state, recovers incomplete parsing, redispatches pending/stale subtasks, finalizes with a lease, and retries completion notifications.

SQS DLQ/redrive configuration is not fully implemented in this branch.

Real AWS integration was not tested locally. The test suite uses local unit tests and fakes only.
