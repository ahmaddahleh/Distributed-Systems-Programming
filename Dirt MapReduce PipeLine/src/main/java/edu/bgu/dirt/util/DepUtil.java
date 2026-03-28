package edu.bgu.dirt.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DepUtil {
    // Appendix A functional marker relations
    // FIXED: Changed Set.of() to new HashSet<>(Arrays.asList()) for Java 8 compatibility
    public static final Set<String> FUNCTIONAL = new HashSet<>(Arrays.asList(
        "det","poss","neg","aux","auxpass","ps","mark","complm","prt"
    ));

    public static boolean isFunctional(String dep) { 
        return dep != null && FUNCTIONAL.contains(dep); 
    }
}