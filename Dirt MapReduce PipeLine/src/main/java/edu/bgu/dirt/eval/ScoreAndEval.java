package edu.bgu.dirt.eval;

import edu.bgu.dirt.util.Args;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Evaluates DIRT-style similarity on the provided positive/negative test set.
 *
 * Required args:
 * --mi=<path>         MI TSV file (typically the filtered MI from FilterMiJob)
 * --pos=<path>        positive predicate pairs TSV (lhs\trhs)
 * --neg=<path>        negative predicate pairs TSV (lhs\trhs)
 * --out=<path>        where to write scores TSV (lhs\trhs\tlabel\tnorm_lhs\tnorm_rhs\tscore)
 *
 * Optional args:
 * --outDir=<path>     directory for best_threshold.txt, pr_curve.tsv, error_analysis.tsv
 * (default: parent directory of --out)
 * --compareWith=<path> another scores TSV (same lhs/rhs/label), to compare small vs large
 * --useMaxSwap=true|false   if true, score max(direct, swapped-alignment). Default: false.
 */
public class ScoreAndEval {

    static class Pair {
        final String a, b;
        final int y;
        Pair(String a, String b, int y) { this.a = a; this.b = b; this.y = y; }
    }

    static class Scored {
        final Pair p;
        final String na, nb;
        final double s;
        Scored(Pair p, String na, String nb, double s) { this.p = p; this.na = na; this.nb = nb; this.s = s; }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    public static int run(String[] args) throws Exception {
        Map<String, String> m = Args.parse(args);

        String miFile = Args.req(m, "mi");
        String pos = Args.req(m, "pos");
        String neg = Args.req(m, "neg");
        String outScores = m.get("out");
        if (outScores == null)
            outScores = m.get("output"); // backward compat with scripts
        if (outScores == null)
            throw new IllegalArgumentException("Missing required arg: --out (or --output)");

        String outDir = m.get("outDir");
        // FIXED: Replaced .isBlank() with .trim().isEmpty() for Java 8
        if (outDir == null || outDir.trim().isEmpty()) {
            Path p = new Path(outScores);
            Path parent = p.getParent();
            outDir = (parent == null) ? "." : parent.toString();
        }

        String compareWith = m.get("compareWith");

        boolean useMaxSwap = Args.bool(m, "useMaxSwap", false);

        Configuration conf = new Configuration();

        MiTable mi = MiTable.load(miFile);

        List<Pair> pairs = new ArrayList<>();
        pairs.addAll(readPairs(conf, pos, 1));
        pairs.addAll(readPairs(conf, neg, 0));

        List<Scored> scored = new ArrayList<>(pairs.size());
        writeScores(conf, outScores, pairs, mi, scored, useMaxSwap);
        printScoreDiagnostics(scored);
        breakdownZeroPositives(mi, scored);
        SweepResult sweep = sweepBestF1(scored);
        ensureDir(conf, new Path(outDir));

        writeBestThreshold(conf, new Path(outDir, "best_threshold.txt"), sweep);
        writePrCurve(conf, new Path(outDir, "pr_curve.tsv"), scored, sweep.P);

        // FIXED: Replaced .isBlank() with .trim().isEmpty() for Java 8
        Map<String, Double> compareMap = (compareWith == null || compareWith.trim().isEmpty())
                ? null
                : readScoreMap(conf, compareWith);
        writeErrorAnalysis(conf, new Path(outDir, "error_analysis.tsv"), scored, sweep.bestThr, compareMap);

        System.err.println("Wrote: " + outScores + ", " + new Path(outDir, "best_threshold.txt") + ", " +
                new Path(outDir, "pr_curve.tsv") + ", " + new Path(outDir, "error_analysis.tsv"));
        return 0;
    }

    private static void ensureDir(Configuration conf, Path dir) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        if (!fs.exists(dir)) fs.mkdirs(dir);
    }

    private static List<Pair> readPairs(Configuration conf, String path, int label) throws IOException {
        List<Pair> out = new ArrayList<>();
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length < 2) continue;
                out.add(new Pair(parts[0], parts[1], label));
            }
        }
        return out;
    }

    private static void writeScores(Configuration conf,
                                    String outScores,
                                    List<Pair> pairs,
                                    MiTable mi,
                                    List<Scored> scoredOut,
                                    boolean useMaxSwap) throws IOException {
        Path outPath = new Path(outScores);
        FileSystem fs = outPath.getFileSystem(conf);
        Path parent = outPath.getParent();
        if (parent != null && !fs.exists(parent)) fs.mkdirs(parent);

        try (FSDataOutputStream os = fs.create(outPath, true);
             Writer w = new OutputStreamWriter(os, StandardCharsets.UTF_8);
             BufferedWriter bw = new BufferedWriter(w)) {
            bw.write("lhs\trhs\tlabel\tnorm_lhs\tnorm_rhs\tscore\n");
            for (Pair p : pairs) {
                TemplateNormalizer.Norm n1 = TemplateNormalizer.normalizeRep(p.a);
                TemplateNormalizer.Norm n2 = TemplateNormalizer.normalizeRep(p.b);
                double s = useMaxSwap
                        ? Similarity.pathSimMaxByRoles(mi, n1, n2)
                        : Similarity.pathSimByRoles(mi, n1, n2);
                scoredOut.add(new Scored(p, n1.logicalNorm, n2.logicalNorm, s));
                bw.write(p.a);
                bw.write('\t');
                bw.write(p.b);
                bw.write('\t');
                bw.write(Integer.toString(p.y));
                bw.write('\t');
                bw.write(n1.logicalNorm);
                bw.write('\t');
                bw.write(n2.logicalNorm);
                bw.write('\t');
                bw.write(String.format(Locale.ROOT, "%.6f", s));
                bw.write('\n');
            }
        }
    }

    static class SweepResult {
        final int P;
        final double bestThr;
        final double bestF1;
        final int bestTP, bestFP, bestFN, bestTN;
        SweepResult(int P, double bestThr, double bestF1, int bestTP, int bestFP, int bestFN, int bestTN) {
            this.P = P;
            this.bestThr = bestThr;
            this.bestF1 = bestF1;
            this.bestTP = bestTP;
            this.bestFP = bestFP;
            this.bestFN = bestFN;
            this.bestTN = bestTN;
        }
    }


    private static void printScoreDiagnostics(List<Scored> scored) {
        int pos = 0, neg = 0;
        int posZero = 0, negZero = 0;
        double minPos = Double.POSITIVE_INFINITY, maxPos = Double.NEGATIVE_INFINITY;
        double minNeg = Double.POSITIVE_INFINITY, maxNeg = Double.NEGATIVE_INFINITY;

        for (Scored r : scored) {
            boolean isPos = (r.p.y == 1);
            if (isPos) {
                pos++;
                if (r.s == 0.0)
                    posZero++;
                minPos = Math.min(minPos, r.s);
                maxPos = Math.max(maxPos, r.s);
            } else {
                neg++;
                if (r.s == 0.0)
                    negZero++;
                minNeg = Math.min(minNeg, r.s);
                maxNeg = Math.max(maxNeg, r.s);
            }
        }

        System.err.println("=== Score diagnostics ===");
        System.err.println("Pos: " + pos + "  Neg: " + neg);
        System.err.println("Pos score range: [" + minPos + ", " + maxPos + "], zeros=" + posZero);
        System.err.println("Neg score range: [" + minNeg + ", " + maxNeg + "], zeros=" + negZero);
        System.err.println("=========================");
    }



       private static void breakdownZeroPositives(MiTable mi, List<Scored> scored) {
        int zeroPos = 0;
        int missP1 = 0, missP2 = 0;
        int emptyX = 0, emptyY = 0;
        int noOverlapX = 0, noOverlapY = 0;

        for (Scored r : scored) {
            if (r.p.y != 1)
                continue;
            if (r.s != 0.0)
                continue;
            zeroPos++;

            TemplateNormalizer.Norm n1 = TemplateNormalizer.normalizeRep(r.p.a);
            TemplateNormalizer.Norm n2 = TemplateNormalizer.normalizeRep(r.p.b);

            // physical slots for logical X/Y (same mapping as Similarity)
            String p1X = n1.swappedXY ? "Y" : "X";
            String p1Y = n1.swappedXY ? "X" : "Y";
            String p2X = n2.swappedXY ? "Y" : "X";
            String p2Y = n2.swappedXY ? "X" : "Y";

            Map<String, Double> v1x = mi.vec(n1.canonicalPid, p1X);
            Map<String, Double> v1y = mi.vec(n1.canonicalPid, p1Y);
            Map<String, Double> v2x = mi.vec(n2.canonicalPid, p2X);
            Map<String, Double> v2y = mi.vec(n2.canonicalPid, p2Y);

            boolean p1Missing = v1x.isEmpty() && v1y.isEmpty();
            boolean p2Missing = v2x.isEmpty() && v2y.isEmpty();
            if (p1Missing)
                missP1++;
            if (p2Missing)
                missP2++;

            if (v1x.isEmpty() || v2x.isEmpty())
                emptyX++;
            else if (intersectionEmpty(v1x, v2x))
                noOverlapX++;

            if (v1y.isEmpty() || v2y.isEmpty())
                emptyY++;
            else if (intersectionEmpty(v1y, v2y))
                noOverlapY++;
        }

        System.err.println("=== Zero-positive breakdown ===");
        System.err.println("zeroPos=" + zeroPos);
        System.err.println("missing predicate vectors: p1=" + missP1 + " p2=" + missP2);
        System.err.println("empty slot vectors: X=" + emptyX + " Y=" + emptyY);
        System.err.println("non-empty but no-overlap: X=" + noOverlapX + " Y=" + noOverlapY);
        System.err.println("==============================");
    }

    private static boolean intersectionEmpty(Map<String, Double> a, Map<String, Double> b) {
        Map<String, Double> small = a.size() <= b.size() ? a : b;
        Map<String, Double> big = a.size() <= b.size() ? b : a;
        for (String k : small.keySet()) {
            if (big.containsKey(k))
                return false;
        }
        return true;
    }

    /**
     * Finds the threshold (score >= thr => predict positive) that maximizes F1.
     * Evaluates thresholds at each distinct score value.
     */
    private static SweepResult sweepBestF1(List<Scored> scored) {
        List<Scored> sorted = new ArrayList<>(scored);
        sorted.sort((u, v) -> Double.compare(v.s, u.s));

        int P = 0;
        for (Scored r : sorted) if (r.p.y == 1) P++;

        int tp = 0, fp = 0, seen = 0;
        double bestF1 = -1.0, bestThr = 0.0;
        int bestTP = 0, bestFP = 0, bestFN = 0, bestTN = 0;

        double prev = Double.NaN;
        for (Scored r : sorted) {
            double s = r.s;
            if (seen > 0 && Double.compare(s, prev) != 0) {
                int fn = P - tp;
                int tn = (sorted.size() - seen) - fn;
                double prec = (tp + fp) == 0 ? 0 : (tp * 1.0 / (tp + fp));
                double rec = P == 0 ? 0 : (tp * 1.0 / P);
                double f1 = (prec + rec) == 0 ? 0 : (2 * prec * rec / (prec + rec));
                if (f1 > bestF1) {
                    bestF1 = f1;
                    bestThr = prev;
                    bestTP = tp;
                    bestFP = fp;
                    bestFN = fn;
                    bestTN = tn;
                }
            }
            if (r.p.y == 1) tp++; else fp++;
            seen++;
            prev = s;
        }

        // Final point (all predicted positive)
        int fn = P - tp;
        int tn = (sorted.size() - seen) - fn;
        double prec = (tp + fp) == 0 ? 0 : (tp * 1.0 / (tp + fp));
        double rec = P == 0 ? 0 : (tp * 1.0 / P);
        double f1 = (prec + rec) == 0 ? 0 : (2 * prec * rec / (prec + rec));
        if (f1 > bestF1) {
            bestF1 = f1;
            bestThr = prev;
            bestTP = tp;
            bestFP = fp;
            bestFN = fn;
            bestTN = tn;
        }

        return new SweepResult(P, bestThr, bestF1, bestTP, bestFP, bestFN, bestTN);
    }

    private static void writeBestThreshold(Configuration conf, Path out, SweepResult r) throws IOException {
        FileSystem fs = out.getFileSystem(conf);
        try (FSDataOutputStream os = fs.create(out, true);
             Writer w = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            w.write(String.format(Locale.ROOT,
                    "best_threshold\t%.6f\n" +
                            "best_f1\t%.6f\n" +
                            "TP\t%d\n" +
                            "FP\t%d\n" +
                            "FN\t%d\n" +
                            "TN\t%d\n",
                    r.bestThr, r.bestF1, r.bestTP, r.bestFP, r.bestFN, r.bestTN));
        }
    }

    private static void writePrCurve(Configuration conf, Path out, List<Scored> scored, int P) throws IOException {
        List<Scored> sorted = new ArrayList<>(scored);
        sorted.sort((u, v) -> Double.compare(v.s, u.s));

        FileSystem fs = out.getFileSystem(conf);
        try (FSDataOutputStream os = fs.create(out, true);
             Writer w = new OutputStreamWriter(os, StandardCharsets.UTF_8);
             BufferedWriter bw = new BufferedWriter(w)) {
            bw.write("threshold\tprecision\trecall\n");
            int tp = 0, fp = 0, seen = 0;
            double prev = Double.NaN;
            for (Scored r : sorted) {
                double s = r.s;
                if (seen > 0 && Double.compare(s, prev) != 0) {
                    double prec = (tp + fp) == 0 ? 0 : (tp * 1.0 / (tp + fp));
                    double rec = P == 0 ? 0 : (tp * 1.0 / P);
                    bw.write(String.format(Locale.ROOT, "%.6f\t%.6f\t%.6f\n", prev, prec, rec));
                }
                if (r.p.y == 1) tp++; else fp++;
                seen++;
                prev = s;
            }
            double prec = (tp + fp) == 0 ? 0 : (tp * 1.0 / (tp + fp));
            double rec = P == 0 ? 0 : (tp * 1.0 / P);
            bw.write(String.format(Locale.ROOT, "%.6f\t%.6f\t%.6f\n", prev, prec, rec));
        }
    }

    /**
     * Map key for comparing runs: (lhs, rhs, label).
     */
    private static String scoreKey(String lhs, String rhs, int label) {
        return lhs + "\t" + rhs + "\t" + label;
    }

    private static Map<String, Double> readScoreMap(Configuration conf, String scoresTsv) throws IOException {
        Map<String, Double> map = new HashMap<>();
        Path p = new Path(scoresTsv);
        FileSystem fs = p.getFileSystem(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    // skip header if present
                    if (line.startsWith("lhs\t")) continue;
                }
                String[] parts = line.split("\t");
                if (parts.length < 6) continue;
                int label;
                try { label = Integer.parseInt(parts[2]); } catch (NumberFormatException e) { continue; }
                double score;
                try { score = Double.parseDouble(parts[5]); } catch (NumberFormatException e) { continue; }
                map.put(scoreKey(parts[0], parts[1], label), score);
            }
        }
        return map;
    }

    private static void writeErrorAnalysis(Configuration conf,
                                           Path out,
                                           List<Scored> scored,
                                           double threshold,
                                           Map<String, Double> compareMap) throws IOException {

        List<Scored> tp = new ArrayList<>();
        List<Scored> fp = new ArrayList<>();
        List<Scored> tn = new ArrayList<>();
        List<Scored> fn = new ArrayList<>();

        for (Scored r : scored) {
            boolean pred = r.s >= threshold;
            boolean gold = r.p.y == 1;
            if (pred && gold) tp.add(r);
            else if (pred) fp.add(r);
            else if (gold) fn.add(r);
            else tn.add(r);
        }

        // Pick representative examples (more informative extremes).
        tp.sort((a, b) -> Double.compare(b.s, a.s));
        fp.sort((a, b) -> Double.compare(b.s, a.s));
        fn.sort((a, b) -> Double.compare(a.s, b.s));
        tn.sort((a, b) -> Double.compare(a.s, b.s));

        FileSystem fs = out.getFileSystem(conf);
        try (FSDataOutputStream os = fs.create(out, true);
             Writer w = new OutputStreamWriter(os, StandardCharsets.UTF_8);
             BufferedWriter bw = new BufferedWriter(w)) {

            if (compareMap != null) {
                bw.write("category\tlhs\trhs\tlabel\tscore\tcompare_score\n");
            } else {
                bw.write("category\tlhs\trhs\tlabel\tscore\n");
            }

            writeExamples(bw, "TP", tp, 5, compareMap);
            writeExamples(bw, "FP", fp, 5, compareMap);
            writeExamples(bw, "TN", tn, 5, compareMap);
            writeExamples(bw, "FN", fn, 5, compareMap);
        }
    }

    private static void writeExamples(BufferedWriter bw,
                                      String cat,
                                      List<Scored> list,
                                      int k,
                                      Map<String, Double> compareMap) throws IOException {
        int n = Math.min(k, list.size());
        for (int i = 0; i < n; i++) {
            Scored r = list.get(i);
            bw.write(cat);
            bw.write('\t');
            bw.write(r.p.a);
            bw.write('\t');
            bw.write(r.p.b);
            bw.write('\t');
            bw.write(Integer.toString(r.p.y));
            bw.write('\t');
            bw.write(String.format(Locale.ROOT, "%.6f", r.s));

            if (compareMap != null) {
                bw.write('\t');
                Double s2 = compareMap.get(scoreKey(r.p.a, r.p.b, r.p.y));
                if (s2 == null) bw.write("NA");
                else bw.write(String.format(Locale.ROOT, "%.6f", s2));
            }
            bw.write('\n');
        }
    }
}