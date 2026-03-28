package edu.bgu.dirt.util;

import java.util.HashMap;
import java.util.Map;

public class Args {
    public static Map<String,String> parse(String[] args) {
        Map<String,String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) continue;
            String k = a.substring(2);
            String v = "true";
            if (i+1 < args.length && !args[i+1].startsWith("--")) v = args[++i];
            m.put(k, v);
        }
        return m;
    }

    public static String req(Map<String,String> m, String k) {
        if (!m.containsKey(k)) throw new IllegalArgumentException("Missing --" + k);
        return m.get(k);
    }

    public static boolean bool(Map<String,String> m, String k, boolean def) {
        if (!m.containsKey(k)) return def;
        return Boolean.parseBoolean(m.get(k));
    }
}
