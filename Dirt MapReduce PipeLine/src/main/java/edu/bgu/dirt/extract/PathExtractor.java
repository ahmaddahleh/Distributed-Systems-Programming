package edu.bgu.dirt.extract;

import edu.bgu.dirt.model.Token;
import edu.bgu.dirt.parse.TreeFragment;
import edu.bgu.dirt.util.*;

import java.util.*;

/**
 * Extract canonical predicate templates:
 * - "X <verbStem> Y"
 * - "X <verbStem> <prep> Y"  where prep is either:
 * (a) an explicit IN/TO token above a pobj/pcomp noun, or
 * (b) a collapsed Stanford relation on the noun edge (e.g., prep_with / prepc_to).
 *
 * Only outputs paths where endpoints are nouns and LCA is a verb.
 */
public class PathExtractor {
    private final PorterStemmer stemmer = new PorterStemmer();
    private final boolean stemAll;
    private final boolean filterAux;

    /**
     * Stanford dependencies typically attach auxiliaries as dependents of the main verb (dep=aux/auxpass),
     * so the LCA head is usually already the real content verb.
     *
     * Still, some fragments may yield auxiliary-like heads. We filter *surface forms* only
     * (not stemmed), so we do NOT accidentally drop legitimate copular templates like "X be in Y"
     * that appear in the provided test set.
     */
    // FIXED: Changed Set.of() to new HashSet<>(Arrays.asList()) for Java 8 compatibility
    private final Set<String> auxSurface = new HashSet<>(Arrays.asList(
            "is","are","was","were","am","been","being",
            "do","does","did",
            "have","has","had"
    ));

    public PathExtractor(boolean stemAll, boolean filterAux) {
        this.stemAll = stemAll;
        this.filterAux = filterAux;
    }

    public List<PathInstance> extract(TreeFragment tf, long totalCount) {
        List<Token> toks = tf.tokens;
        List<Integer> nouns = new ArrayList<>();
        for (int i = 0; i < toks.size(); i++) {
            Token t = toks.get(i);
            if (DepUtil.isFunctional(t.dep)) continue;
            if (PosUtil.isNoun(t.pos)) nouns.add(i);
        }
        if (nouns.size() < 2) return Collections.emptyList();

        List<PathInstance> out = new ArrayList<>();
        for (int a = 0; a < nouns.size(); a++) for (int b = a+1; b < nouns.size(); b++) {
            int i = nouns.get(a), j = nouns.get(b);
            int lca = lca(tf, i, j);
            if (lca < 0) continue;
            Token root = toks.get(lca);
            if (!PosUtil.isVerb(root.pos)) continue;

            String verb = stemmer.stem(root.word);
            if (filterAux) {
                String surf = root.word == null ? "" : root.word.toLowerCase();
                if (auxSurface.contains(surf) || "aux".equals(root.dep) || "auxpass".equals(root.dep)) continue;
            }

            PrepInfo prep = choosePrep(tf, i, j, lca);
            int x = chooseX(tf, i, j, prep);
            int y = (x == i) ? j : i;

            String pathId;
            if (prep != null) {
                // ensure Y is the pobj side (to match templates like "X confuse with Y")
                if (prep.pobjIndex == x) { int tmp = x; x = y; y = tmp; }
                pathId = "X " + verb + " " + prep.prepWord + " Y";
            } else {
                pathId = "X " + verb + " Y";
            }

            String fillX = norm(toks.get(x).word);
            String fillY = norm(toks.get(y).word);

            out.add(new PathInstance(pathId, "X", fillX, totalCount));
            out.add(new PathInstance(pathId, "Y", fillY, totalCount));
        }
        return out;
    }

    private String norm(String w) { return stemAll ? stemmer.stem(w) : w.toLowerCase(); }

    static class PrepInfo {
        final String prepWord; final int prepIndex; final int pobjIndex;
        PrepInfo(String prepWord, int prepIndex, int pobjIndex) {
            this.prepWord = prepWord; this.prepIndex = prepIndex; this.pobjIndex = pobjIndex;
        }
    }

    private PrepInfo choosePrep(TreeFragment tf, int i, int j, int lca) {
        PrepInfo pi = prepOf(tf, i);
        PrepInfo pj = prepOf(tf, j);
        if (pi != null && isAncestor(tf, lca, pi.prepIndex)) return pi;
        if (pj != null && isAncestor(tf, lca, pj.prepIndex)) return pj;
        return pi != null ? pi : pj;
    }

    private PrepInfo prepOf(TreeFragment tf, int nounIdx) {
        int p = tf.parent[nounIdx];
        if (p < 0) return null;
        Token noun = tf.tokens.get(nounIdx);
        Token parent = tf.tokens.get(p);

        // Case 1: explicit preposition token exists in the fragment
        //   ... prep(IN/TO) -> pobj/pcomp(NN)
        if (("pobj".equals(noun.dep) || "pcomp".equals(noun.dep)) && PosUtil.isPrep(parent.pos)) {
            return new PrepInfo(parent.word.toLowerCase(), p, nounIdx);
        }

        // Case 2: collapsed Stanford relation encodes the preposition on the noun edge
        //   ... verb -> prep_with(NN)   or   verb -> prepc_to(NN)
        // In such cases there may be no explicit IN/TO token in the fragment.
        String dep = noun.dep == null ? "" : noun.dep;
        String prep = null;
        if (dep.startsWith("prep_") && dep.length() > "prep_".length()) {
            prep = dep.substring("prep_".length());
        } else if (dep.startsWith("prepc_") && dep.length() > "prepc_".length()) {
            prep = dep.substring("prepc_".length());
        }
        
        // FIXED: Replaced !prep.isBlank() with !prep.trim().isEmpty()
        if (prep != null && !prep.trim().isEmpty()) {
            return new PrepInfo(prep.toLowerCase(), p, nounIdx);
        }

        return null;
    }

    private int chooseX(TreeFragment tf, int i, int j, PrepInfo prep) {
        Token ti = tf.tokens.get(i), tj = tf.tokens.get(j);
        // Dataset variants sometimes use older Stanford labels (subj/subjpass) vs UD-style (nsubj/nsubjpass).
        boolean iSubj = "nsubj".equals(ti.dep) || "nsubjpass".equals(ti.dep) || "subj".equals(ti.dep) || "subjpass".equals(ti.dep);
        boolean jSubj = "nsubj".equals(tj.dep) || "nsubjpass".equals(tj.dep) || "subj".equals(tj.dep) || "subjpass".equals(tj.dep);
        if (iSubj && !jSubj) return i;
        if (jSubj && !iSubj) return j;
        if (prep != null) {
            if (prep.pobjIndex == i) return j;
            if (prep.pobjIndex == j) return i;
        }
        return Math.min(i, j);
    }

    private boolean isAncestor(TreeFragment tf, int anc, int node) {
        int cur = node;
        boolean[] seen = new boolean[tf.tokens.size()];
        int steps = 0;
        while (cur >= 0 && steps <= tf.tokens.size()) {
            if (cur == anc) return true;
            if (seen[cur]) break; // cycle guard
            seen[cur] = true;
            cur = tf.parent[cur];
            steps++;
        }
        return false;
    }

    private int lca(TreeFragment tf, int a, int b) {
        int n = tf.tokens.size();
        boolean[] seen = new boolean[n];

        int cur = a;
        int steps = 0;
        while (cur >= 0 && steps <= n) {
            if (seen[cur]) break; // cycle guard
            seen[cur] = true;
            cur = tf.parent[cur];
            steps++;
        }

        cur = b;
        steps = 0;
        while (cur >= 0 && steps <= n) {
            if (seen[cur]) return cur;
            // also guard cycles on the b-chain
            if (tf.parent[cur] == cur) break;
            cur = tf.parent[cur];
            steps++;
        }
        return -1;
    }
}