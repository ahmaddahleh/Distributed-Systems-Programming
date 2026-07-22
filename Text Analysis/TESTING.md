# Testing

Run local tests from the Maven module:

```bash
cd code
mvn clean test
```

Build the shaded JAR locally:

```bash
cd code
mvn clean package
```

These commands do not intentionally create AWS resources. Do not run `LocalApplication`, `Manager`, or `Worker` unless you intend to use real AWS services.

## Current coverage

The unit tests cover:

- deterministic `subTaskId` generation
- malformed input skipping
- deterministic and distinct result keys
- Worker SQS acknowledgement order
- Manager result acknowledgement order
- idempotent Manager aggregation
- duplicate and conflicting result handling
- HTML escaping and safe URL rendering
- runtime configuration defaults and overrides
- exactly-once EC2 user-data encoding helper
- durable Manager state transitions and recovery windows
- local job request acknowledgement after durable persistence
- stale dispatch redispatch
- leased report finalization
- completion notification retry

## Not covered locally

The following require AWS or a future integration-test harness:

- DynamoDB table existence and IAM permissions
- IAM permissions
- EC2 instance launch success
- S3 object upload/download success
- SQS queue attributes, visibility timeout, and DLQ redrive behavior
- Stanford CoreNLP performance on EC2 instance sizes
