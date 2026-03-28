package edu.bgu.dirt.eval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MiTable {
    private final Map<String, Map<String, Map<String, Double>>> mi = new HashMap<>();
    private final Map<String, Map<String, Double>> slotSum = new HashMap<>();

    public static MiTable load(String tsvPath) throws IOException {
        MiTable t = new MiTable();

        Configuration conf = new Configuration();
        Path p = new Path(tsvPath);
        FileSystem fs = p.getFileSystem(conf);

        FileStatus[] initial = fs.globStatus(p);

        // If globStatus didn't match anything, try direct exists.
        if (initial == null) {
            if (!fs.exists(p)) return t;
            initial = new FileStatus[]{ fs.getFileStatus(p) };
        }

        // Expand directories into their contained files.
        List<FileStatus> files = new ArrayList<>();
        for (FileStatus st : initial) {
            if (st == null) continue;
            if (st.isDirectory()) {
                FileStatus[] kids = fs.listStatus(st.getPath());
                if (kids != null) {
                    for (FileStatus k : kids) {
                        if (k != null && k.isFile()) files.add(k);
                    }
                }
            } else if (st.isFile()) {
                files.add(st);
            }
        }

        // Deterministic order helps reproducibility.
        files.sort(Comparator.comparing(s -> s.getPath().toString()));

        long kept = 0;
        for (FileStatus st : files) {
            String name = st.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(st.getPath()), StandardCharsets.UTF_8))) {

                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length < 5) continue; // pid slot w mi count

                    String pid = parts[0], slot = parts[1], w = parts[2];
                    double miVal;
                    try { miVal = Double.parseDouble(parts[3]); }
                    catch (NumberFormatException e) { continue; }

                    // Keep only positive PMI weights (DIRT-style).
                    if (miVal <= 0.0) continue;

                    t.mi.computeIfAbsent(pid, k -> new HashMap<>())
                            .computeIfAbsent(slot, k -> new HashMap<>())
                            .put(w, miVal);
                    t.slotSum.computeIfAbsent(pid, k -> new HashMap<>())
                            .merge(slot, miVal, Double::sum);
                    kept++;
                }
            }
        }

        System.err.println("MiTable.load: kept " + kept + " positive MI entries from " + tsvPath);
        return t;
    }

    public Map<String, Double> vec(String pid, String slot) {
        return mi.getOrDefault(pid, Collections.emptyMap())
                 .getOrDefault(slot, Collections.emptyMap());
    }

    public double sum(String pid, String slot) {
        return slotSum.getOrDefault(pid, Collections.emptyMap())
                      .getOrDefault(slot, 0.0);
    }
}
