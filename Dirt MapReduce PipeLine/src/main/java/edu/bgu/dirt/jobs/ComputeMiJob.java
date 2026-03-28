package edu.bgu.dirt.jobs;

import edu.bgu.dirt.util.Args;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Job3: compute MI(p, slot, w) for every (p,slot,w) observed in the corpus.
 *
 * MI formula (assignment Eq.1):
 *   MI(p,slot,w) = log( (A * D) / (B * C) )
 * where:
 *   A = |p,slot,w|
 *   B = |p,slot,*|
 *   C = |*,slot,w|
 *   D = |*,slot,*|
 *
 * Performance improvements (major speedups on large corpora):
 *   1) Do NOT accidentally run with 1 reducer: we auto-estimate reducers based on input size,
 *      and allow overriding via --reducers.
 *   2) Avoid regex split / String.format in the tight loop.
 *   3) LRU-cache slot-word totals (MapFile lookups) because words repeat heavily.
 */
public class ComputeMiJob extends Configured implements Tool {

    public static final String CONF_SLOT_WORD_CACHE_SIZE = "dirt.computeMi.slotWordCacheSize";
    public static final int DEFAULT_SLOT_WORD_CACHE_SIZE = 1_000_000;

    private static final long TARGET_BYTES_PER_REDUCER = 256L * 1024L * 1024L;
    private static final int MIN_REDUCERS = 4;
    private static final int MAX_REDUCERS = 200;

    /** Simple primitive growable long array (avoids boxing). */
    static final class LongVec {
        private long[] a;
        private int n;
        LongVec(int cap) { a = new long[Math.max(8, cap)]; }
        void clear() { n = 0; }
        void add(long v) {
            if (n == a.length) a = Arrays.copyOf(a, a.length * 2);
            a[n++] = v;
        }
        long get(int i) { return a[i]; }
        int size() { return n; }
    }

    /** Simple LRU cache. */
    static final class LruCache<K, V> extends LinkedHashMap<K, V> {
        private final int max;
        LruCache(int max) {
            super((int) (max / 0.75f) + 1, 0.75f, true);
            this.max = max;
        }
        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > max;
        }
    }

    public static class M extends Mapper<Object, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            // Input line: pid \t slot \t word \t count
            // Fast tab parsing (no regex split)
            String line = value.toString();
            int t1 = line.indexOf('\t');
            if (t1 < 0) return;
            int t2 = line.indexOf('\t', t1 + 1);
            if (t2 < 0) return;
            int t3 = line.indexOf('\t', t2 + 1);
            if (t3 < 0) return;

            // Key: pid\tslot   Value: word\tcount
            outK.set(line.substring(0, t2));
            outV.set(line.substring(t2 + 1));
            ctx.write(outK, outV);
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        private Map<String, Long> slotTotals;
        private MapFile.Reader slotWordReader;

        private final Text outK = new Text();
        private final Text outV = new Text();
        private final Text swKey = new Text();
        private final LongWritable swVal = new LongWritable();

        private final StringBuilder outSb = new StringBuilder(64);
        private final StringBuilder swSb = new StringBuilder(64);

        private ArrayList<String> words;
        private LongVec counts;

        private LruCache<String, Long> cacheX;
        private LruCache<String, Long> cacheY;

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();

            slotTotals = loadSlotTotals(conf, new Path(conf.get("dirt.slotTotals")));

            Path userPath = new Path(conf.get("dirt.slotWordMap"));
            FileSystem fs = userPath.getFileSystem(conf);
            Path mapPath = resolveSingleMapFileDir(fs, userPath);
            slotWordReader = new MapFile.Reader(fs, mapPath.toString(), conf);

            int cacheTotal = conf.getInt(CONF_SLOT_WORD_CACHE_SIZE, DEFAULT_SLOT_WORD_CACHE_SIZE);
            int perSlot = Math.max(10_000, cacheTotal / 2);
            cacheX = new LruCache<>(perSlot);
            cacheY = new LruCache<>(perSlot);

            words = new ArrayList<>(256);
            counts = new LongVec(256);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException {
            if (slotWordReader != null) slotWordReader.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            // key: pid\tslot
            String k = key.toString();
            int tab = k.indexOf('\t');
            if (tab < 0) return;

            String pid = k.substring(0, tab);
            String slot = k.substring(tab + 1);

            long D = slotTotals.getOrDefault(slot, 0L);
            if (D == 0) return;

            words.clear();
            counts.clear();

            long B = 0; // |p,slot,*|
            for (Text t : vals) {
                // value: word\tcount
                String s = t.toString();
                int tTab = s.lastIndexOf('\t');
                if (tTab < 0) continue;

                String w = s.substring(0, tTab);
                long A;
                try {
                    A = Long.parseLong(s.substring(tTab + 1));
                } catch (NumberFormatException e) {
                    continue;
                }

                words.add(w);
                counts.add(A);
                B += A;
            }
            if (B == 0) return;

            for (int i = 0; i < words.size(); i++) {
                String w = words.get(i);
                long A = counts.get(i);

                long C = getSlotWordTotal(slot, w);
                if (C <= 0) continue;

                double mi = Math.log(((double) A * (double) D) / ((double) B * (double) C));
                if (!(mi > 0.0)) continue;  // skips mi <= 0 and also skips NaN


                // out key: pid\tslot\tword
                outSb.setLength(0);
                outSb.append(pid).append('\t').append(slot).append('\t').append(w);
                outK.set(outSb.toString());

                // out value: mi\tcount
                outSb.setLength(0);
                outSb.append(mi).append('\t').append(A);
                outV.set(outSb.toString());

                ctx.write(outK, outV);

                if ((i & 0x3FFF) == 0) ctx.progress();
            }
        }

        private long getSlotWordTotal(String slot, String w) throws IOException {
            LruCache<String, Long> cache = "X".equals(slot) ? cacheX : cacheY;
            Long v = cache.get(w);
            if (v != null) return v;

            // MapFile key: slot\tword
            swSb.setLength(0);
            swSb.append(slot).append('\t').append(w);
            swKey.set(swSb.toString());

            swVal.set(0);
            if (slotWordReader.get(swKey, swVal) == null) {
                cache.put(w, 0L);
                return 0L;
            }

            long C = swVal.get();
            cache.put(w, C);
            return C;
        }

        private Map<String, Long> loadSlotTotals(Configuration conf, Path folder) throws IOException {
            Map<String, Long> m = new HashMap<>();
            FileSystem fs = folder.getFileSystem(conf);

            for (FileStatus st : fs.listStatus(folder)) {
                if (st == null || !st.isFile()) continue;
                String name = st.getPath().getName();
                if (name.startsWith("_") || name.startsWith(".")) continue;

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(st.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        int t = line.indexOf('\t');
                        if (t < 0) continue;
                        String s = line.substring(0, t);
                        long c;
                        try {
                            c = Long.parseLong(line.substring(t + 1).trim());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        Long prev = m.get(s);
                        m.put(s, prev == null ? c : prev + c);
                    }
                }
            }
            return m;
        }

        /**
         * MapFileOutputFormat writes a MapFile directory.
         * Caller might pass either:
         *   1) .../slot_word/part-r-00000  (the MapFile dir)
         *   2) .../slot_word              (parent dir containing part-r-00000)
         */
        private Path resolveSingleMapFileDir(FileSystem fs, Path p) throws IOException {
            if (fs.exists(new Path(p, "data")) && fs.exists(new Path(p, "index"))) return p;
            if (!fs.exists(p) || !fs.getFileStatus(p).isDirectory()) return p;

            FileStatus[] sts = fs.listStatus(p);
            Path fallback = null;
            for (FileStatus st : sts) {
                if (st == null || !st.isDirectory()) continue;
                String name = st.getPath().getName();
                if (!name.startsWith("part-")) continue;

                Path cand = st.getPath();
                if (fs.exists(new Path(cand, "data")) && fs.exists(new Path(cand, "index"))) {
                    if ("part-r-00000".equals(name)) return cand;
                    if (fallback == null) fallback = cand;
                }
            }
            return fallback != null ? fallback : p;
        }
    }

    public static boolean checkIfExists(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            if (!fs.delete(outputPath, true)) {
                throw new IOException("Failed to delete existing output path: " + outputPath);
            }
            return true;
        }
        return false;
    }

    @Override
    public int run(String[] args) throws Exception {
        Map<String, String> m = Args.parse(args);

        String triples = Args.req(m, "triples");
        String slotWordMap = Args.req(m, "slotWordMap");
        String slotTotals = Args.req(m, "slotTotals");
        String out = Args.req(m, "output");

        int reducers = parseInt(m.get("reducers"), -1);
        int cacheSize = parseInt(m.get("slotWordCacheSize"), DEFAULT_SLOT_WORD_CACHE_SIZE);

        Configuration conf = getConf();
        conf.set("dirt.slotWordMap", slotWordMap);
        conf.set("dirt.slotTotals", slotTotals);
        conf.setInt(CONF_SLOT_WORD_CACHE_SIZE, cacheSize);
        conf.setBoolean("mapreduce.map.output.compress", true);

        Job job = Job.getInstance(conf, "DIRT Job3 ComputeMI");
        job.setJarByClass(ComputeMiJob.class);

        // Avoid duplicate work in unstable clusters.
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);

        if (reducers <= 0) reducers = estimateReducers(conf, new Path(triples));
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(triples));
        Path outPath = new Path(out);
        checkIfExists(conf, outPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static int parseInt(String s, int def) {
        if (s == null) return def;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    private static int estimateReducers(Configuration conf, Path input) throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        long bytes = sumBytes(fs, input);
        if (bytes <= 0) return 1;

        int r = (int) ((bytes + TARGET_BYTES_PER_REDUCER - 1) / TARGET_BYTES_PER_REDUCER);
        r = Math.max(MIN_REDUCERS, r);
        r = Math.min(MAX_REDUCERS, r);
        return r;
    }

    private static long sumBytes(FileSystem fs, Path p) throws IOException {
        if (!fs.exists(p)) return 0;
        FileStatus st = fs.getFileStatus(p);

        if (st.isFile()) {
            String name = p.getName();
            if (name.startsWith("_") || name.startsWith(".")) return 0;
            return st.getLen();
        }

        long sum = 0;
        for (FileStatus kid : fs.listStatus(p)) {
            if (kid == null) continue;
            Path kp = kid.getPath();
            String name = kp.getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;
            sum += kid.isDirectory() ? sumBytes(fs, kp) : kid.getLen();
        }
        return sum;
    }
}
