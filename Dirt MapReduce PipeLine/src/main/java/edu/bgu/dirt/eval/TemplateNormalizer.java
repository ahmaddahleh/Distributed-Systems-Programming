package edu.bgu.dirt.eval;

import edu.bgu.dirt.util.PorterStemmer;

import java.util.Locale;

/**
 * Normalizes test-set templates (e.g., "X produce by Y", "Y result from X")
 * into a form that can be used to look up MI vectors.
 *
 * IMPORTANT:
 * The MI table is keyed by a canonical predicate string that always starts with "X".
 * However, the test set sometimes writes predicates in the swapped form "Y ... X".
 *
 * If we blindly normalize everything to "X ... Y" without remembering that swap,
 * we accidentally treat a predicate and its inverse as identical
 * (e.g., "X convert to Y" == "Y convert to X"), which produces wrong high scores.
 *
 * This class therefore returns:
 *  - canonicalPid: always "X <verbStem> [prep...] Y" (for MI lookup)
 *  - swappedXY: true iff the ORIGINAL template started with Y (meaning logical X/Y are swapped)
 *  - logicalNorm: a human-readable normalized string that preserves the original X/Y order
 */
public class TemplateNormalizer {
    private static final PorterStemmer stemmer = new PorterStemmer();

    public static final class Norm {
        public final String canonicalPid; // always starts with X, ends with Y
        public final boolean swappedXY;   // true iff original template was "Y ... X"
        public final String logicalNorm;  // normalized but preserves original variable order

        Norm(String canonicalPid, boolean swappedXY, String logicalNorm) {
            this.canonicalPid = canonicalPid;
            this.swappedXY = swappedXY;
            this.logicalNorm = logicalNorm;
        }
    }

    /**
     * Backward-compatible API: returns the canonical "X ... Y" form.
     */
    public static String normalize(String template) {
        return normalizeRep(template).canonicalPid;
    }

    /**
     * Full normalization with swap-awareness.
     */
    public static Norm normalizeRep(String template) {
        if (template == null) return new Norm("", false, "");

        // Lowercase + collapse whitespace.
        String cleaned = template.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        String[] t = cleaned.split(" ");
        if (t.length < 3) {
            // Too short / malformed: keep as-is.
            return new Norm(cleaned, false, cleaned);
        }

        // Detect whether the template is written as "Y ... X".
        // Test set is expected to start with X or Y.
        boolean startsWithY = "y".equals(t[0]);
        boolean startsWithX = "x".equals(t[0]);
        boolean swapped = startsWithY && !startsWithX;

        // Find the second variable token position (prefer the LAST x/y token).
        int var2Idx = -1;
        for (int i = 1; i < t.length; i++) {
            if ("x".equals(t[i]) || "y".equals(t[i])) var2Idx = i;
        }
        if (var2Idx == -1 || var2Idx == 0) {
            return new Norm(cleaned, swapped, cleaned);
        }

        // Verb is expected right after the first variable token.
        int verbIdx = 1;
        if (verbIdx >= t.length) return new Norm(cleaned, swapped, cleaned);
        String verbStem = stemmer.stem(t[verbIdx]);

        // Preposition phrase = everything between verb and the second variable token.
        String prepPhrase = null;
        if (verbIdx + 1 < var2Idx) {
            StringBuilder sb = new StringBuilder();
            for (int i = verbIdx + 1; i < var2Idx; i++) {
                if (sb.length() > 0) sb.append(' ');
                sb.append(t[i]);
            }
            if (sb.length() > 0) prepPhrase = sb.toString();
        }

        // Canonical form for MI lookup is always "X ... Y".
        String canonical;
        if (prepPhrase != null) canonical = "X " + verbStem + " " + prepPhrase + " Y";
        else canonical = "X " + verbStem + " Y";

        // Logical normalized form keeps the original X/Y order (for readability in scores.tsv).
        String firstVar = startsWithY ? "Y" : "X";
        String secondVar = "x".equals(t[var2Idx]) ? "X" : "Y";
        String logical;
        if (prepPhrase != null) logical = firstVar + " " + verbStem + " " + prepPhrase + " " + secondVar;
        else logical = firstVar + " " + verbStem + " " + secondVar;

        // If the input was malformed and didn't start with X/Y, don't claim swapped.
        if (!startsWithX && !startsWithY) swapped = false;

        return new Norm(canonical, swapped, logical);
    }
}
