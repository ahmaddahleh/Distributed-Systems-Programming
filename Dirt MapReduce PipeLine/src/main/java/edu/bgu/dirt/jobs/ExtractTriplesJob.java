package edu.bgu.dirt.jobs;

import edu.bgu.dirt.extract.PathExtractor;
import edu.bgu.dirt.extract.PathInstance;
import edu.bgu.dirt.model.NgramRecord;
import edu.bgu.dirt.parse.NgramLineParser;
import edu.bgu.dirt.parse.TreeFragment;
import edu.bgu.dirt.util.Args;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExtractTriplesJob extends Configured implements Tool {

    public static class M extends Mapper<LongWritable, Text, Text, LongWritable> {
        private PathExtractor extractor;
        private final Text outK = new Text();
        private final LongWritable outV = new LongWritable();

        // Light-weight heartbeat counter to avoid long task-timeout on rare heavy records.
        private long heartbeat = 0;

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            extractor = new PathExtractor(conf.getBoolean("dirt.stemAll", true),
                                          conf.getBoolean("dirt.filterAux", false));
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Always report some progress at the beginning of each record.
            // If a single record is malformed or very expensive, this helps prevent
            // Hadoop's default 10-minute task timeout.
            if ((heartbeat++ & 0x3FF) == 0) ctx.progress();

            NgramRecord rec = NgramLineParser.parse(value.toString());
            if (rec == null) return;

            TreeFragment tf = new TreeFragment(rec.tokens);
            List<PathInstance> inst = extractor.extract(tf, rec.totalCount);

            // Emit (pathId,slot,filler) -> count for both slots.
            for (PathInstance pi : inst) {
                outK.set(pi.pathId + "\t" + pi.slot + "\t" + pi.filler);
                outV.set(pi.count);
                ctx.write(outK, outV);
                // Heartbeat every ~1K outputs (keeps overhead low).
                if ((heartbeat++ & 0x3FF) == 0) ctx.progress();
            }
        }
    }

    public static class R extends Reducer<Text, LongWritable, Text, LongWritable> {
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
        String out = Args.req(m, "output");

        Configuration conf = getConf();
        conf.setBoolean("dirt.stemAll", Args.bool(m, "stemAll", true));
        // Recommended in the assignment: filter auxiliary-like heads (still overridable).
        conf.setBoolean("dirt.filterAux", Args.bool(m, "filterAux", true));

        Job job = Job.getInstance(conf, "DIRT Job1 ExtractTriples");
        job.setJarByClass(ExtractTriplesJob.class);

        // Counters are used elsewhere; speculative execution can cause over-counting.
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);

        job.setMapperClass(M.class);
        job.setCombinerClass(R.class);
        job.setReducerClass(R.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        for (String p : in.split(",")) {
            String t = p.trim();
           if (!t.isEmpty() && !t.equals("s3://meni-biarcs/biarcs.99-of-99.gz")) {
                FileInputFormat.addInputPath(job, new Path(t));
            }
        }

        Path outPath = new Path(out);
        checkIfExists(conf, outPath);
        FileOutputFormat.setOutputPath(job, outPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
