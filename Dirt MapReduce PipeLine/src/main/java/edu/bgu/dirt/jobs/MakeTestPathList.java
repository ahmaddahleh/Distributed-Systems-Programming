package edu.bgu.dirt.jobs;

import edu.bgu.dirt.eval.TemplateNormalizer;
import edu.bgu.dirt.util.Args;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MakeTestPathList {
    public static int run(String[] args) throws Exception {
        Map<String,String> m = Args.parse(args);
        String pos = Args.req(m, "pos");
        String neg = Args.req(m, "neg");
        String out = Args.req(m, "output");

        Configuration conf = new Configuration();

        Set<String> s = new TreeSet<>();
        addFile(conf, pos, s);
        addFile(conf, neg, s);

        Path outPath = new Path(out);
        FileSystem outFs = outPath.getFileSystem(conf);
        try (FSDataOutputStream os = outFs.create(outPath, true);
             Writer w = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            for (String p : s) w.write(p + "\n");
        }
        System.err.println("Wrote " + s.size() + " paths to " + out);
        return 0;
    }

    private static void addFile(Configuration conf, String path, Set<String> out) throws IOException {
        Path inPath = new Path(path);
        FileSystem fs = inPath.getFileSystem(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.split("\t");
                if (p.length < 2) continue;
                out.add(TemplateNormalizer.normalize(p[0]));
                out.add(TemplateNormalizer.normalize(p[1]));
            }
        }
    }
}
