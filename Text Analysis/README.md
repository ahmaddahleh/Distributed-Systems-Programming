# ☁️ Distributed Text Analysis in the Cloud

A distributed Java project that analyzes text files in the cloud using **AWS EC2, S3, SQS**, and **Stanford CoreNLP**.  
The system follows a **Manager–Worker architecture**, where a local client submits analysis jobs, a manager coordinates the workflow, and worker instances process the text in parallel.

---

## 📌 Overview

This project was built for the **Distributed Systems Programming** assignment. The goal is to process a list of text-file URLs, analyze each file using a requested NLP operation, upload the results to AWS S3, and generate a final HTML summary file. The assignment requires a local application, a manager, and workers communicating through SQS and S3. :contentReference[oaicite:0]{index=0}

---

## ✨ What the system does

For each input line, the system receives:

- an **analysis type**
- a **URL** to a text file

Supported analysis types:

- `POS` — Part-of-Speech tagging
- `CONSTITUENCY` — Constituency parsing
- `DEPENDENCY` — Dependency parsing

The sample input format in the assignment is exactly `<analysis type>\t<URL>`, such as `POS`, `CONSTITUENCY`, and `DEPENDENCY` followed by Project Gutenberg text URLs. 

---

## 🏗️ Architecture

The project is built from 3 main components:

### 1. Local Application
Runs on the user’s machine and is responsible for:
- checking whether a Manager instance is already running
- uploading the input file to S3
- sending a job request to SQS
- waiting for the final result
- downloading the generated HTML summary

### 2. Manager
Runs on an EC2 instance and is responsible for:
- receiving jobs from local applications
- downloading the input file from S3
- splitting the work into analysis tasks
- launching Worker instances when needed
- sending tasks to workers via SQS
- collecting worker results
- building the final HTML summary
- notifying the local application when the job is done

### 3. Workers
Run on EC2 instances and are responsible for:
- receiving tasks from the manager
- downloading remote text files
- running the requested NLP analysis
- uploading result files to S3
- reporting success or failure back to the manager

This design matches the assignment’s required flow of Local Application → Manager → Workers, using SQS for messaging and S3 for storage. 

---

## 🧠 How it works

1. The local application uploads the input file to S3.
2. It sends a `newTask` message to the Manager through SQS.
3. The Manager downloads the input file and parses each line into a worker task.
4. The Manager calculates how many workers are needed based on `n` (max tasks per worker).
5. Worker instances pull tasks from the shared worker queue.
6. Each Worker downloads the target text file and runs one of the supported Stanford CoreNLP analyses.
7. The Worker uploads the output file to S3.
8. The Worker sends a completion or failure message back to the Manager.
9. Once all subtasks are complete, the Manager generates an HTML summary file and uploads it to S3.
10. The Local Application downloads the final HTML result.

This is the same general pipeline described in the assignment summary. :contentReference[oaicite:3]{index=3}

---

## 🛠️ Technologies Used
Java 17

Maven

AWS EC2

AWS S3

AWS SQS

Stanford CoreNLP

org.json

---

## ⚙️ Main Components in the Code
### LocalApplication.java

Handles the client-side flow:

-uploads the input file

-ensures a Manager exists

-sends a task message

-waits for the final summary

-downloads the output HTML

-Manager.java

### Acts as the orchestrator:

-creates/uses SQS queues

-parses the input file

-launches workers

-dispatches tasks

-aggregates results

-uploads the final summary HTML

### Worker.java

Executes individual NLP tasks:

-downloads a text file from a URL

-runs the requested analysis

-uploads the result to S3

-reports back to the manager

### StanfordAnalyzer.java

Wraps Stanford CoreNLP and exposes:

-POS analysis

-Constituency parsing

-Dependency parsing

### AWS.java

Central helper for AWS clients and EC2 instance creation.

## 🚀 How to Run
1. Build the project
mvn clean package
2. Run the Local Application
mvn -q exec:java -Dexec.mainClass="dsp1.LocalApplication.LocalApplication" -Dexec.args="input-sample.txt output.html 3"

Optional terminate mode
mvn -q exec:java -Dexec.mainClass="dsp1.LocalApplication.LocalApplication" -Dexec.args="input-sample.txt output.html 3 terminate"

## 📝 Arguments

inputFileName outputFileName n [terminate]

inputFileName — input file from src/main/resources

outputFileName — name of the final HTML file

n — maximum number of tasks per worker

terminate — optional flag to request shutdown after finishing

The assignment also defines the same overall runtime interface with inputFileName, outputFileName, n, and optional terminate.

## 📄 Example Input

POS    https://www.gutenberg.org/files/1659/1659-0.txt

CONSTITUENCY    https://www.gutenberg.org/files/1659/1659-0.txt

DEPENDENCY    https://www.gutenberg.org/files/1659/1659-0.txt

## 📤 Output

The system generates an HTML summary file containing:

-the analysis type

-a link to the original input file

-a link to the analyzed output file
or an error description if processing failed

This matches the output format required in the assignment.

## 🔐 Notes

This project uses AWS services, so valid AWS access and permissions are required.

Do not upload credentials, .pem files, or secret keys to GitHub.

The project relies on IAM roles / AWS environment configuration at runtime.

The current implementation includes hardcoded AWS resource names such as the S3 bucket and queue names, so these may need adjustment before running in another environment.

## 📈 Current Configuration

Based on the current code:

AWS region: us-east-1

EC2 instance type: t2.medium

Main S3 bucket: dsp-ahmad-dah

⚠️ Important Implementation Note

This repository reflects the project implementation as submitted.
One thing worth knowing is that Workers delete their queue message immediately after receiving it, so full crash-retry behavior is not as strong as the ideal SQS visibility-timeout pattern described in the assignment. The project still reports failures back to the Manager, but this is a limitation of the current implementation.

👥 Authors

Ahmad Dahleh

Ebrahim Taha
