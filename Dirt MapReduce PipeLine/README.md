# 🔎 DIRT MapReduce Pipeline

A Java + Hadoop MapReduce implementation of a **DIRT-style lexico-syntactic similarity system** for extracting inference rules from text.

This project processes **Google Syntactic N-Grams (biarcs)** data, extracts dependency-based predicate paths, computes **mutual information (MI)** for `(path, slot, word)` triples, and evaluates similarity on a provided **positive/negative predicate test set**.

---

## 📌 What this project does

This system implements a DIRT-style pipeline that:

- Extracts predicate templates such as:
  - `X cause Y`
  - `X result from Y`
  - `X confuse with Y`
- Builds frequency statistics for path-slot-word triples
- Computes **MI(path, slot, word)**
- Scores similarity between predicate pairs from the test set
- Produces evaluation artifacts such as:
  - best F1 threshold
  - precision-recall curve
  - error analysis

The project follows the assignment requirements of:
- producing `mi(p,slot,w)` values
- scoring each predicate pair in the test set
- supporting experiments on **small (10 files)** and **large (100 files)** inputs. 

---

## 🧠 Pipeline Overview

The system is built as a 6-step pipeline:

1. **`extract-triples`**  
   Parse biarcs lines and extract `(pathId, slot, filler) -> count`

2. **`global-counts`**  
   Aggregate global slot-word counts and slot totals

3. **`compute-mi`**  
   Compute positive MI values for extracted triples

4. **`make-test-path-list`**  
   Build a whitelist of normalized predicate templates from the test set

5. **`filter-mi`**  
   Keep only MI entries relevant to the evaluation set

6. **`score-eval`**  
   Compute similarity scores and generate evaluation files

---

## 🛠 Tech Stack

- **Java 8**
- **Apache Hadoop MapReduce**
- **Maven**
- **AWS EMR** runner included
- Custom implementation of:
  - path extraction
  - template normalization
  - Porter stemming
  - DIRT-style similarity scoring

---

## ▶️ Build
mvn -DskipTests package

After building, the main shaded jar should be available at:

target/dirt-mr-1.0-SNAPSHOT-all.jar

---

## 🚀 Available Commands

### The main jar supports these commands:

extract-triples

global-counts

compute-mi

make-test-path-list

filter-mi

score-eval

### General usage:

java -jar target/dirt-mr-1.0-SNAPSHOT-all.jar <command> [args]

---

## 🧪 Example Pipeline Run
1. Extract triples
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar extract-triples \
  --input s3a://YOUR_BUCKET/biarcs_small/ \
  --output s3a://YOUR_BUCKET/out/job1_triples \
  --stemAll true \
  --filterAux true
2. Compute global counts
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar global-counts \
  --input s3a://YOUR_BUCKET/out/job1_triples \
  --output hdfs:///tmp/dirt/global_counts
3. Compute MI
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar compute-mi \
  --triples s3a://YOUR_BUCKET/out/job1_triples \
  --slotWordMap hdfs:///tmp/dirt/global_counts/slot_word/part-r-00000 \
  --slotTotals hdfs:///tmp/dirt/global_counts/slot_totals \
  --output s3a://YOUR_BUCKET/out/job3_mi
4. Build test path list
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar make-test-path-list \
  --pos s3a://YOUR_BUCKET/testset/positive-preds.txt \
  --neg s3a://YOUR_BUCKET/testset/negative-preds.txt \
  --output hdfs:///tmp/dirt_test_paths.txt
5. Filter MI
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar filter-mi \
  --mi s3a://YOUR_BUCKET/out/job3_mi \
  --paths hdfs:///tmp/dirt_test_paths.txt \
  --output s3a://YOUR_BUCKET/out/job3_mi_filtered
6. Score and evaluate
hadoop jar target/dirt-mr-1.0-SNAPSHOT-all.jar score-eval \
  --mi s3a://YOUR_BUCKET/out/job3_mi_filtered \
  --pos s3a://YOUR_BUCKET/testset/positive-preds.txt \
  --neg s3a://YOUR_BUCKET/testset/negative-preds.txt \
  --out s3a://YOUR_BUCKET/out/scores.tsv \
  --outDir s3a://YOUR_BUCKET/out/eval

---

## ☁️ AWS EMR Runner

This project also includes an AWS EMR runner under:

runner/src/main/java/RunDirtAws.java

It supports:

running the classic 3-step MR pipeline

running the full 6-step pipeline

submitting single commands to EMR

Example idea:

java -jar runner.jar <jarS3> pipeline-all <inputS3> <outputS3> <posS3> <negS3>

There is also a helper script:

scripts/emr_steps.sh

---

## 📥 Input

The system expects:

Google Syntactic N-Grams biarcs data

a positive predicate pairs file

a negative predicate pairs file

Included locally:

testset/positive-preds.txt

testset/negative-preds.txt

## 📤 Output

The pipeline generates:

MI table
format: pathId\t slot\t word\t mi\t count

scores.tsv
format: lhs\trhs\tlabel\tnorm_lhs\tnorm_rhs\tscore

best_threshold.txt

pr_curve.tsv

error_analysis.tsv

These outputs support both assignment evaluation and further similarity experiments.

---

## ✨ Project Highlights

DIRT-style similarity over dependency paths

Handles X/Y role-aware comparison

Supports swapped predicate directions

Uses Porter stemming

Filters auxiliary verbs

Designed for scalable processing with Hadoop/EMR

---

## 📚 Notes

The extracted predicate templates are normalized into canonical forms like X <verb> Y or X <verb> <prep> Y.

Evaluation supports both direct alignment and role-aware comparisons.

The system is meant for academic experimentation and analysis on small and large corpora.
