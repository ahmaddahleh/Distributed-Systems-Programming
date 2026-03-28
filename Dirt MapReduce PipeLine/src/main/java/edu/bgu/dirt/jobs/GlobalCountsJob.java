package edu.bgu.dirt.jobs;

import edu.bgu.dirt.util.Args;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.Map;

public class GlobalCountsJob extends Configured implements Tool {

    /**
     * Counters used to compute slot totals D(slot, *).
     *
     * We only expect slot in {X,Y}. Using job-level counters avoids writing
     * O(|slot,word|) lines and keeps slot_totals output tiny.
     */
    public static final String SLOT_TOTALS_GROUP = "slot_totals";

    public static class M extends Mapper<Object, Text, Text, LongWritable> {
        private final Text outK = new Text();
        private final LongWritable outV = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            // job1 line: p slot w count
            String[] p = value.toString().split("\t");
            if (p.length < 4) return;
            String slot = p[1], w = p[2];
            long c;
            try { c = Long.parseLong(p[3]); } catch (NumberFormatException e) { return; }

            // D(slot,*) = sum_{p,w} A(p,slot,w)
            // We only expect slot values X/Y, but count whatever we see.
            ctx.getCounter(SLOT_TOTALS_GROUP, slot).increment(c);

            outK.set(slot + "\t" + w);
            outV.set(c);
            ctx.write(outK, outV);
        }
    }

    /**
     * Pure sum reducer (safe to use as a combiner).
     */
    public static class SumR extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable outV = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : vals) sum += v.get();
            outV.set(sum);
            ctx.write(key, outV);
        }
    }

    public static boolean checkIfExists(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            // recursive delete (directory)
            if (!fs.delete(outputPath, true)) {
                throw new IOException("Failed to delete existing output path: " + outputPath);
            }
            return true;
        }
        return false;
    }



    @Override
    public int run(String[] args) throws Exception {
        Map<String,String> m = Args.parse(args);
        String in = Args.req(m, "input");
        String out = Args.req(m, "output"); // put on HDFS for MapFile random access

        Job job = Job.getInstance(getConf(), "DIRT Job2 GlobalCounts");
        job.setJarByClass(GlobalCountsJob.class);

        // For correctness and to simplify downstream MapFile reads, we force a single reducer.
        // (Otherwise MapFileOutputFormat emits one MapFile per reducer, requiring a multi-map reader.)
        job.setNumReduceTasks(1);

        // Speculative execution can cause over-counting when using job counters for slot totals.
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);

        job.setMapperClass(M.class);
        job.setCombinerClass(SumR.class);
        job.setReducerClass(SumR.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(in));
        Path outPath = new Path(out + "/slot_word");
        checkIfExists(job.getConfiguration(), outPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        boolean ok = job.waitForCompletion(true);
        if (!ok) return 1;

        // Write slot totals (D) as a tiny HDFS/S3 file for Job3 random access.
        long xTotal = job.getCounters().findCounter(SLOT_TOTALS_GROUP, "X").getValue();
        long yTotal = job.getCounters().findCounter(SLOT_TOTALS_GROUP, "Y").getValue();

        Configuration conf = job.getConfiguration();
        Path totalsDir = new Path(out + "/slot_totals");
        FileSystem fs = totalsDir.getFileSystem(conf);
        if (!fs.exists(totalsDir)) fs.mkdirs(totalsDir);

        Path totalsFile = new Path(totalsDir, "part-00000");
        try (FSDataOutputStream os = fs.create(totalsFile, true);
             OutputStreamWriter w = new OutputStreamWriter(os);
             BufferedWriter bw = new BufferedWriter(w)) {
            bw.write("X\t" + xTotal + "\n");
            bw.write("Y\t" + yTotal + "\n");
        }

        return 0;
    }
}
