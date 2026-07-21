# Distributed Text Analysis

Java 17 / Maven project for distributed text analysis using AWS EC2, S3, SQS, and Stanford CoreNLP.

## Architecture

1. `LocalApplication` uploads an input file to S3 and sends a `newTask` message to `LocalToManagerQueue`.
2. `Manager` downloads the input file, parses valid lines, assigns each line a stable `subTaskId`, and sends worker tasks to `ManagerToWorkersQueue`.
3. `Worker` receives one task, downloads the source text URL, runs Stanford CoreNLP, uploads the result to S3, and sends `jobDone` or `failedjob` to `WorkersToManagerQueue`.
4. `Manager` records each terminal result once by `subTaskId`, generates a safe escaped HTML summary, uploads it to S3, and notifies `LocalApplication` through `ManagerToLocalQueue`.
5. `LocalApplication` downloads the summary HTML.

## Build And Test

From the Maven module:

```bash
cd code
mvn clean test
mvn clean package
```

The test suite is local-only and does not deploy AWS resources.

## Configuration

Defaults preserve the assignment setup. Override with Java system properties or environment variables.

| Setting | System property | Environment variable | Default |
|---|---|---|---|
| AWS region | `dsp.aws.region` | `DSP_AWS_REGION` | `us-east-1` |
| S3 bucket | `dsp.aws.bucket` | `DSP_AWS_BUCKET` | `dsp-ahmad-dah` |
| EC2 AMI | `dsp.aws.ami` | `DSP_AWS_AMI` | `ami-0c02fb55956c7d316` |
| EC2 key pair | `dsp.aws.keyPair` | `DSP_AWS_KEY_PAIR` | `vockey` |
| IAM profile | `dsp.aws.iamProfile` | `DSP_AWS_IAM_PROFILE` | `LabInstanceProfile` |
| Worker limit | `dsp.worker.limit` | `DSP_WORKER_LIMIT` | `7` |

Use `.env.example` as a placeholder reference only. Do not commit real credentials.

## Safe Local Workflow

Use `mvn clean test` for local verification. Do not run the distributed application unless you intend to contact AWS.

Running `LocalApplication`, `Manager`, or `Worker` may create AWS charges because the code can create queues, upload S3 objects, launch EC2 instances, or terminate instances when requested.

## Reliability Behavior

Worker task messages are acknowledged only after processing reaches a terminal result and the result notification is sent successfully.

Manager worker-result messages are acknowledged only after the Manager records the terminal result.

Each valid input line gets a deterministic `subTaskId` using:

```text
taskId + ":" + validTaskIndex
```

The Manager treats `subTaskId` as the logical unit of completion. Duplicate or conflicting terminal results are ignored after the first accepted result.

## Known Limitations

Manager state remains in memory. A Manager restart during an active job still loses active job state.

Full production recovery, durable job state, and SQS DLQ/redrive handling are not implemented in this focused branch.

Real AWS integration was not tested locally.
