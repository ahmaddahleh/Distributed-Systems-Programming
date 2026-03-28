package edu.bgu.dirt.parse;

import edu.bgu.dirt.model.Token;
import java.util.ArrayList;
import java.util.List;

public class TreeFragment {
    public final List<Token> tokens;
    public final int[] parent; // -1 root
    public final List<Integer>[] children;

    @SuppressWarnings("unchecked")
    public TreeFragment(List<Token> tokens) {
        this.tokens = tokens;
        int n = tokens.size();
        parent = new int[n];
        children = new List[n];
        for (int i = 0; i < n; i++) children[i] = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int head1 = tokens.get(i).headIndex; // 1-based, 0 root
            if (head1 == 0) parent[i] = -1;
            else {
                int p = head1 - 1;
                // Defensive parsing: if the fragment is malformed, head indices can create cycles.
                // A token cannot be its own head in a valid dependency tree.
                if (p == i) {
                    parent[i] = -1;
                } else {
                    parent[i] = (p >= 0 && p < n) ? p : -1;
                }
                if (parent[i] != -1) children[parent[i]].add(i);
            }
        }
    }
}
