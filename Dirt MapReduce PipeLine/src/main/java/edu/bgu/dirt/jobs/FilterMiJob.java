package edu.bgu.dirt.jobs;

import edu.bgu.dirt.util.Args;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FilterMiJob extends Configured implements Tool {

    // Write the whole MI line as the KEY so TextOutputFormat prints it cleanly.
    public static class M extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Set<String> keep;

        @Override
        protected void setup(Context ctx) throws IOException {
            keep = new HashSet<>();
            Configuration conf = ctx.getConfiguration();
            Path pathsFile = new Path(conf.get("dirt.paths"));
            FileSystem fs = pathsFile.getFileSystem(conf);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathsFile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) keep.add(line);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] p = value.toString().split("\t");
            if (p.length < 5) return;       // pid slot w mi count
            if (!keep.contains(p[0])) return;
            ctx.write(value, NullWritable.get());
        }
    }

    private static void deleteIfExists(Configuration conf, Path outPath) throws IOException {
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            if (!fs.delete(outPath, true)) {
                throw new IOException("Failed to delete existing output path: " + outPath);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Map<String,String> m = Args.parse(args);
        String mi = Args.req(m, "mi");
        String paths = Args.req(m, "paths");
        String out = Args.req(m, "output");

        Configuration conf = getConf();
        conf.set("dirt.paths", paths);

        Job job = Job.getInstance(conf, "DIRT FilterMI");
        job.setJarByClass(FilterMiJob.class);
        job.setMapperClass(M.class);
        job.setNumReduceTasks(0);

        // Print ONLY the key (no tab separator).
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(mi));
        Path outPath = new Path(out);
        deleteIfExists(conf, outPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
