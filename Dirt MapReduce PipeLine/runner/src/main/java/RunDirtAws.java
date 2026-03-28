import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.*;

public class RunDirtAws {

    private static void sleep(int sec) {
        try { Thread.sleep(sec * 1000L); } catch (InterruptedException ignored) {}
    }

    private static String findStepIdByName(AmazonElasticMapReduce emr, String clusterId, String stepName) {
        ListStepsResult r = emr.listSteps(new ListStepsRequest().withClusterId(clusterId));
        List<StepSummary> steps = r.getSteps();
        if (steps == null || steps.isEmpty())
            throw new RuntimeException("No steps found");
        for (StepSummary s : steps) {
            if (stepName.equals(s.getName()))
                return s.getId();
        }
        throw new RuntimeException("Step not found: " + stepName);
    }

    private static String stepState(AmazonElasticMapReduce emr, String clusterId, String stepId) {
        return emr.describeStep(
                new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId)
        ).getStep().getStatus().getState();
    }

    private static boolean isTerminal(String state) {
        return "COMPLETED".equals(state)
                || "FAILED".equals(state)
                || "CANCELLED".equals(state)
                || "INTERRUPTED".equals(state);
    }

    private static void usageAndExit() {
        System.err.println("Usage:");
        System.err.println("  (legacy MR pipeline, 3 steps)");
        System.err.println("    java -jar runner.jar <jarS3> <inputS3> <outputS3> [instanceCount]");
        System.err.println();
        System.err.println("  (explicit MR pipeline, 3 steps)");
        System.err.println("    java -jar runner.jar <jarS3> pipeline-mr <inputS3> <outputS3> [instanceCount] [--instances N] [--stemAll ...] [--filterAux ...]");
        System.err.println();
        System.err.println("  (full pipeline, 6 steps)");
        System.err.println("    java -jar runner.jar <jarS3> pipeline-all <inputS3> <outputS3> <posS3> <negS3> [instanceCount] [--instances N] [--stemAll ...] [--filterAux ...]");
        System.err.println();
        System.err.println("  (single command)");
        System.err.println("    java -jar runner.jar <jarS3> <command> <commandArgs...> [instanceCount] [--instances N]");
        System.err.println("    command ∈ { extract-triples, global-counts, compute-mi, make-test-path-list, filter-mi, score-eval }");
        System.exit(1);
    }

    private static boolean isInt(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private static int extractInstances(List<String> tokens) {
        // Runner-only: --instances N
        for (int i = 0; i < tokens.size(); i++) {
            if ("--instances".equals(tokens.get(i))) {
                if (i + 1 >= tokens.size()) throw new IllegalArgumentException("Missing value after --instances");
                int n = Integer.parseInt(tokens.get(i + 1));
                tokens.remove(i);
                tokens.remove(i);
                return n;
            }
        }
        // Back-compat: trailing integer instanceCount
        if (!tokens.isEmpty()) {
            String last = tokens.get(tokens.size() - 1);
            if (isInt(last)) {
                tokens.remove(tokens.size() - 1);
                return Integer.parseInt(last);
            }
        }
        return 3;
    }

    private static Map<String, String> parseKeyValueArgs(List<String> tokens) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < tokens.size(); i++) {
            String t = tokens.get(i);
            if (t.startsWith("--")) {
                String key = t.substring(2);
                String val = "true";
                if (i + 1 < tokens.size() && !tokens.get(i + 1).startsWith("--")) {
                    val = tokens.get(i + 1);
                    i++;
                }
                m.put(key, val);
            }
        }
        return m;
    }

    private static String normPrefix(String p) {
        if (p == null) return null;
        while (p.endsWith("/")) p = p.substring(0, p.length() - 1);
        return p;
    }

    private static String[] toArray(List<String> a) {
        return a.toArray(new String[0]);
    }

    public static void main(String[] args) {

        if (args.length < 3) usageAndExit();

        final String jarS3 = args[0];

        final Set<String> commands = new HashSet<>(Arrays.asList(
                "extract-triples", "global-counts", "compute-mi",
                "make-test-path-list", "filter-mi", "score-eval"
        ));

        final String modeOrInput = args[1];

        // ---- AWS / EMR CONFIG ----
        String region  = "us-east-1";
        String keyName = "vockey";
        String logUri  = "s3://dirt-ahmad-2025/logs/";

        AmazonElasticMapReduce emr =
                AmazonElasticMapReduceClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(new ProfileCredentialsProvider())
                        .build();

        // Tail tokens (everything after jarS3 + modeOrInput)
        List<String> tail = new ArrayList<>();
        for (int i = 2; i < args.length; i++) tail.add(args[i]);

        int instances;
        List<StepConfig> stepsToRun = new ArrayList<>();
        String lastStepNameToWaitFor;

        // =========================
        // FULL 6-STEP PIPELINE
        // =========================
        if ("pipeline-all".equals(modeOrInput) || "all".equals(modeOrInput)) {

            instances = extractInstances(tail);

            // Positional:
            // pipeline-all <input> <output> <pos> <neg> [instanceCount] ...
            if (tail.size() < 4) usageAndExit();

            String input  = tail.get(0);
            String output = normPrefix(tail.get(1));
            String pos    = tail.get(2);
            String neg    = tail.get(3);

            // Remaining tokens after positional (may include --stemAll/--filterAux)
            List<String> rest = (tail.size() > 4) ? new ArrayList<>(tail.subList(4, tail.size())) : new ArrayList<>();
            Map<String, String> kv = parseKeyValueArgs(rest);

            // forward ONLY to extract-triples
            List<String> extractExtra = new ArrayList<>();
            if (kv.containsKey("stemAll"))   { extractExtra.add("--stemAll");   extractExtra.add(kv.get("stemAll")); }
            if (kv.containsKey("filterAux")) { extractExtra.add("--filterAux"); extractExtra.add(kv.get("filterAux")); }

            // Per-step output folders (all UNDER <output>)
            String triplesOut     = output + "/01_triples";
            String miOut          = output + "/03_mi";
            String testPathDir    = output + "/04_testpath";
            String pathsOut       = testPathDir + "/paths.txt";
            String miFilteredOut  = output + "/05_filter";
            String scoreDir       = output + "/06_score";
            String scoresOut      = scoreDir + "/scores.tsv";

            // Keep global-counts MapFile on HDFS (fast random access in compute-mi)
            String hdfsBase  = "hdfs:///tmp/dirt/" + System.currentTimeMillis();
            String countsOut = hdfsBase + "/02_global_counts";

            // Step 1: extract-triples (S3 -> S3)
            List<String> step1Args = new ArrayList<>();
            step1Args.add("extract-triples");
            step1Args.add("--input");  step1Args.add(input);
            step1Args.add("--output"); step1Args.add(triplesOut);
            step1Args.addAll(extractExtra);

            StepConfig step1 = new StepConfig()
                    .withName("DIRT 01 extract-triples")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(toArray(step1Args)))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            // Step 2: global-counts (S3 -> HDFS)
            StepConfig step2 = new StepConfig()
                    .withName("DIRT 02 global-counts")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "global-counts",
                                    "--input", triplesOut,
                                    "--output", countsOut
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            // Step 3: compute-mi (S3 + HDFS -> S3)
            StepConfig step3 = new StepConfig()
                    .withName("DIRT 03 compute-mi")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "compute-mi",
                                    "--triples", triplesOut,
                                    "--slotWordMap", countsOut + "/slot_word",
                                    "--slotTotals", countsOut + "/slot_totals",
                                    "--output", miOut
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            // Step 4: make-test-path-list (S3 -> S3 file)
            StepConfig step4 = new StepConfig()
                    .withName("DIRT 04 make-test-path-list")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "make-test-path-list",
                                    "--pos", pos,
                                    "--neg", neg,
                                    "--output", pathsOut
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            // Step 5: filter-mi (S3 -> S3 folder)
            StepConfig step5 = new StepConfig()
                    .withName("DIRT 05 filter-mi")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "filter-mi",
                                    "--mi", miOut,
                                    "--paths", pathsOut,
                                    "--output", miFilteredOut
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            // Step 6: score-eval (S3 -> S3 folder + file)
            StepConfig step6 = new StepConfig()
                    .withName("DIRT 06 score-eval")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "score-eval",
                                    "--mi", miFilteredOut + "/part-*",
                                    "--pos", pos,
                                    "--neg", neg,
                                    "--out", scoresOut,
                                    "--outDir", scoreDir
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            stepsToRun.add(step1);
            stepsToRun.add(step2);
            stepsToRun.add(step3);
            stepsToRun.add(step4);
            stepsToRun.add(step5);
            stepsToRun.add(step6);

            lastStepNameToWaitFor = "DIRT 03 compute-mi";
        }

        // =========================
        // MR 3-STEP PIPELINE (legacy + pipeline-mr)
        // =========================
        else if ("pipeline-mr".equals(modeOrInput) || "mr".equals(modeOrInput) || "pipeline".equals(modeOrInput)
                || (!commands.contains(modeOrInput))) {

            String input;
            String output;
            List<String> rest;

            if (!commands.contains(modeOrInput)) {
                // legacy: <jarS3> <inputS3> <outputS3> [instanceCount]
                input  = modeOrInput;
                output = normPrefix(args[2]);
                rest = new ArrayList<>();
                for (int i = 3; i < args.length; i++) rest.add(args[i]);
            } else {
                // explicit pipeline-mr <input> <output> ...
                if (tail.size() < 2) usageAndExit();
                input  = tail.get(0);
                output = normPrefix(tail.get(1));
                rest = (tail.size() > 2) ? new ArrayList<>(tail.subList(2, tail.size())) : new ArrayList<>();
            }

            instances = extractInstances(rest);
            Map<String, String> kv = parseKeyValueArgs(rest);

            List<String> extractExtra = new ArrayList<>();
            if (kv.containsKey("stemAll"))   { extractExtra.add("--stemAll");   extractExtra.add(kv.get("stemAll")); }
            if (kv.containsKey("filterAux")) { extractExtra.add("--filterAux"); extractExtra.add(kv.get("filterAux")); }

            String triplesOut = output + "/01_triples";
            String miOut      = output + "/03_mi";

            String hdfsBase  = "hdfs:///tmp/dirt/" + System.currentTimeMillis();
            String countsOut = hdfsBase + "/02_global_counts";

            List<String> step1Args = new ArrayList<>();
            step1Args.add("extract-triples");
            step1Args.add("--input");  step1Args.add(input);
            step1Args.add("--output"); step1Args.add(triplesOut);
            step1Args.addAll(extractExtra);

            StepConfig step1 = new StepConfig()
                    .withName("DIRT 01 extract-triples")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(toArray(step1Args)))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            StepConfig step2 = new StepConfig()
                    .withName("DIRT 02 global-counts")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs("global-counts", "--input", triplesOut, "--output", countsOut))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            StepConfig step3 = new StepConfig()
                    .withName("DIRT 03 compute-mi")
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(
                                    "compute-mi",
                                    "--triples", triplesOut,
                                    "--slotWordMap", countsOut + "/slot_word",
                                    "--slotTotals", countsOut + "/slot_totals",
                                    "--output", miOut
                            ))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            stepsToRun.add(step1);
            stepsToRun.add(step2);
            stepsToRun.add(step3);

            lastStepNameToWaitFor = "DIRT 03 compute-mi";
        }

        // =========================
        // SINGLE COMMAND
        // =========================
        else {
            instances = extractInstances(tail);

            List<String> passThrough = new ArrayList<>();
            passThrough.add(modeOrInput);
            passThrough.addAll(tail);

            String stepName = "DIRT " + modeOrInput;

            StepConfig step = new StepConfig()
                    .withName(stepName)
                    .withHadoopJarStep(new HadoopJarStepConfig()
                            .withJar(jarS3)
                            .withMainClass("edu.bgu.dirt.cli.Main")
                            .withArgs(toArray(passThrough)))
                    .withActionOnFailure("TERMINATE_CLUSTER");

            stepsToRun.add(step);
            lastStepNameToWaitFor = stepName;
        }

        // ---- Cluster ----
        JobFlowInstancesConfig instancesCfg = new JobFlowInstancesConfig()
                .withEc2KeyName("vockey")
                .withInstanceCount(instances)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge");

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("dirt-run")
                .withReleaseLabel("emr-6.10.0")
                .withApplications(new Application().withName("Hadoop"))
                .withInstances(instancesCfg)
                .withSteps(stepsToRun)
                .withLogUri("s3://dirt-ahmad-2025/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        try {
            RunJobFlowResult result = emr.runJobFlow(request);
            String clusterId = result.getJobFlowId();
            String stepId = findStepIdByName(emr, clusterId, lastStepNameToWaitFor);

            System.out.println("ClusterId: " + clusterId);
            System.out.println("Waiting on step: " + lastStepNameToWaitFor);
            System.out.println("StepId: " + stepId);
            System.out.println("Waiting...");

            while (true) {
                String state = stepState(emr, clusterId, stepId);
                System.out.println("State: " + state);
                if (isTerminal(state)) break;
                sleep(30);
            }

            System.out.println("Done.");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
