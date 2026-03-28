#!/usr/bin/env bash
set -euo pipefail

JAR=${1:-target/dirt-mr-1.0-SNAPSHOT-all.jar}

# You provide:
#   - small input prefix (10 files) and large input prefix (100 files)
#   - test-set pair files (positive + negative)
#   - output prefix
SMALL_INPUT=${2:-s3a://YOUR_BUCKET/biarcs_small/}
LARGE_INPUT=${3:-s3a://YOUR_BUCKET/biarcs_large/}
POS=${4:-s3a://YOUR_BUCKET/testset/positive-preds.txt}
NEG=${5:-s3a://YOUR_BUCKET/testset/negative-preds.txt}
OUT=${6:-s3a://YOUR_BUCKET/out}


# 0) Build the whitelist of predicate templates from the test set
hadoop jar "$JAR" make-test-path-list --pos "$POS" --neg "$NEG" --output hdfs:///tmp/dirt_test_paths.txt

run_pipeline () {
  local NAME=$1
  local INPUT=$2
  local HDFS_BASE=hdfs:///tmp/dirt_${NAME}

  # Job1
  hadoop jar "$JAR" extract-triples --input "$INPUT" --output "$OUT/$NAME/job1_triples" --stemAll true --filterAux true

  # Job2 (write MapFile + slot totals to HDFS for random access in Job3)
  hadoop jar "$JAR" global-counts --input "$OUT/$NAME/job1_triples" --output "${HDFS_BASE}/global_counts"

  # Job3
  # MapFileOutputFormat writes a directory: slot_word/part-r-00000/{data,index}
  # ComputeMiJob auto-detects both forms, but passing the MapFile dir explicitly is clearer.
  hadoop jar "$JAR" compute-mi \
    --triples "$OUT/$NAME/job1_triples" \
    --slotWordMap "${HDFS_BASE}/global_counts/slot_word/part-r-00000" \
    --slotTotals "${HDFS_BASE}/global_counts/slot_totals" \
    --output "$OUT/$NAME/job3_mi"

  # Filter MI to only paths in the test set (keeps evaluation memory small)
  hadoop jar "$JAR" filter-mi --mi "$OUT/$NAME/job3_mi" --paths hdfs:///tmp/dirt_test_paths.txt --output "$OUT/$NAME/job3_mi_filtered"

  # Score + PR + best threshold + error analysis
  hadoop jar "$JAR" score-eval \
    --mi "$OUT/$NAME/job3_mi_filtered" \
    --pos "$POS" \
    --neg "$NEG" \
    --output "$OUT/$NAME/scores.tsv" \
    --outDir "$OUT/$NAME/eval"
}

run_pipeline small "$SMALL_INPUT"
run_pipeline large "$LARGE_INPUT"

# Produce large error analysis with small-vs-large score comparison (compare_score column)
hadoop jar "$JAR" score-eval \
  --mi "$OUT/large/job3_mi_filtered" \
  --pos "$POS" \
  --neg "$NEG" \
  --output "$OUT/large/scores.tsv" \
  --outDir "$OUT/large/eval" \
  --compareWith "$OUT/small/scores.tsv"
