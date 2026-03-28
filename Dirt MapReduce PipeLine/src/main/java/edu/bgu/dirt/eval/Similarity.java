package edu.bgu.dirt.eval;

import java.util.Map;

public class Similarity {
    public static double slotSim(MiTable t, String p1, String p2, String slot) {
        Map<String, Double> v1 = t.vec(p1, slot);
        Map<String, Double> v2 = t.vec(p2, slot);
        if (v1.isEmpty() || v2.isEmpty()) return 0.0;
        double denom = t.sum(p1, slot) + t.sum(p2, slot);
        if (denom <= 0) return 0.0;
        Map<String, Double> small = v1.size() <= v2.size() ? v1 : v2;
        Map<String, Double> big = v1.size() <= v2.size() ? v2 : v1;
        double num = 0.0;
        
        // FIXED: Replaced 'var' with explicit type for Java 8
        for (Map.Entry<String, Double> e : small.entrySet()) {
            Double b = big.get(e.getKey());
            if (b != null) num += e.getValue() + b;
        }
        return num / denom;
    }

    public static double pathSim(MiTable t, String p1, String p2) {
        double sx = slotSim(t, p1, p2, "X");
        double sy = slotSim(t, p1, p2, "Y");
        return Math.sqrt(sx * sy);
    }

    /**
     * Role-aware similarity:
     *
     * The MI table is stored for canonical predicates ("X ... Y").
     * If the test template was written as "Y ... X", we mark it as swappedXY.
     *
     * This method computes similarity *according to the explicit roles in the test templates*
     * (i.e., it respects whether a predicate is written as X..Y or Y..X).
     */
    public static double pathSimByRoles(MiTable t,
                                        TemplateNormalizer.Norm p1,
                                        TemplateNormalizer.Norm p2) {
        double sx = slotSimByRoles(t, p1, p2, "X", "X");
        double sy = slotSimByRoles(t, p1, p2, "Y", "Y");
        return Math.sqrt(sx * sy);
    }

    /**
     * Optional "max over alignments" version (kept for backwards compatibility).
     *
     * WARNING: When used, inverse relations like "X convert to Y" vs "Y convert to X"
     * may get very high scores (because swapped alignment can make them look identical).
     */
    public static double pathSimMaxByRoles(MiTable t,
                                           TemplateNormalizer.Norm p1,
                                           TemplateNormalizer.Norm p2) {
        double direct = pathSimByRoles(t, p1, p2);
        double swapped = Math.sqrt(
                slotSimByRoles(t, p1, p2, "X", "Y") *
                slotSimByRoles(t, p1, p2, "Y", "X")
        );
        return Math.max(direct, swapped);
    }

    private static String physSlot(TemplateNormalizer.Norm p, String logicalSlot) {
        if (!p.swappedXY) return logicalSlot;
        return "X".equals(logicalSlot) ? "Y" : "X";
    }

    private static double slotSimByRoles(MiTable t,
                                         TemplateNormalizer.Norm p1,
                                         TemplateNormalizer.Norm p2,
                                         String logical1,
                                         String logical2) {
        String s1 = physSlot(p1, logical1);
        String s2 = physSlot(p2, logical2);
        return slotSim(t, p1.canonicalPid, p2.canonicalPid, s1, s2);
    }

    // Same computation as slotSim(...) but allows choosing *different* slots for each predicate.
    private static double slotSim(MiTable t, String p1, String p2, String slot1, String slot2) {
        java.util.Map<String, Double> v1 = t.vec(p1, slot1);
        java.util.Map<String, Double> v2 = t.vec(p2, slot2);
        if (v1.isEmpty() || v2.isEmpty()) return 0.0;
        double denom = t.sum(p1, slot1) + t.sum(p2, slot2);
        if (denom <= 0) return 0.0;
        java.util.Map<String, Double> small = v1.size() <= v2.size() ? v1 : v2;
        java.util.Map<String, Double> big = v1.size() <= v2.size() ? v2 : v1;
        double num = 0.0;

        for (java.util.Map.Entry<String, Double> e : small.entrySet()) {
            Double b = big.get(e.getKey());
            if (b != null) num += e.getValue() + b;
        }
        return num / denom;
    }

    /**
     * Some predicate pairs in the provided test set reverse the X/Y roles
     * (e.g., "X cause Y" vs "Y result from X").
     *
     * To avoid missing such pairs, we score both alignments and take the max:
     * - direct:   X->X and Y->Y
     * - swapped:  X->Y and Y->X
     */
    public static double pathSimMax(MiTable t, String p1, String p2) {
        double direct = pathSim(t, p1, p2);
        // swapped alignment
        double sxy = slotSimCross(t, p1, p2);
        return Math.max(direct, sxy);
    }

    private static double slotSimCross(MiTable t, String p1, String p2) {
        double sx = slotSimCross(t, p1, p2, "X", "Y");
        double sy = slotSimCross(t, p1, p2, "Y", "X");
        return Math.sqrt(sx * sy);
    }

    public static double slotSimCross(MiTable t, String p1, String p2, String slot1, String slot2) {
        Map<String, Double> v1 = t.vec(p1, slot1);
        Map<String, Double> v2 = t.vec(p2, slot2);
        if (v1.isEmpty() || v2.isEmpty()) return 0.0;
        double denom = t.sum(p1, slot1) + t.sum(p2, slot2);
        if (denom <= 0) return 0.0;
        Map<String, Double> small = v1.size() <= v2.size() ? v1 : v2;
        Map<String, Double> big = v1.size() <= v2.size() ? v2 : v1;
        double num = 0.0;
        
        // FIXED: Replaced 'var' with explicit type for Java 8
        for (Map.Entry<String, Double> e : small.entrySet()) {
            Double b = big.get(e.getKey());
            if (b != null) num += e.getValue() + b;
        }
        return num / denom;
    }
}