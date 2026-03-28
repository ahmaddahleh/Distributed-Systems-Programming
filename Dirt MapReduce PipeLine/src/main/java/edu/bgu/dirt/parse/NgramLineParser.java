package edu.bgu.dirt.parse;

import edu.bgu.dirt.model.NgramRecord;
import edu.bgu.dirt.model.Token;

import java.util.ArrayList;
import java.util.List;

public class NgramLineParser {
    public static NgramRecord parse(String line) {
        // Format (Google Syntactic N-Grams):
        //   head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
        // We only need the first 3 columns (counts_by_year is ignored).
        //
        // IMPORTANT: Tokens are encoded as:
        //   word/POS/dep-label/head-index
        // The 'word' field can contain '/' characters, so we MUST parse from the right.

        if (line == null) return null;
        int t1 = line.indexOf('\t');
        if (t1 < 0) return null;
        int t2 = line.indexOf('\t', t1 + 1);
        if (t2 < 0) return null;
        int t3 = line.indexOf('\t', t2 + 1);
        // t3 may be -1 if counts_by_year is missing; we still accept 3 columns.

        String head = line.substring(0, t1).trim();
        String ngram = line.substring(t1 + 1, t2).trim();
        String countStr = (t3 < 0) ? line.substring(t2 + 1).trim() : line.substring(t2 + 1, t3).trim();

        long totalCount;
        try {
            totalCount = Long.parseLong(countStr);
        } catch (NumberFormatException e) {
            return null;
        }

        if (ngram.isEmpty()) return null;
        String[] tokStrs = ngram.split(" ");
        List<Token> tokens = new ArrayList<>(tokStrs.length);

        for (String tok : tokStrs) {
            Token t = parseToken(tok);
            if (t == null) {
                // Do NOT keep partially-parsed fragments: head-index references are positional.
                // Dropping a token would corrupt the indexing and can create cycles.
                return null;
            }
            tokens.add(t);
        }

        if (tokens.isEmpty()) return null;
        return new NgramRecord(head, tokens, totalCount);
    }

    private static Token parseToken(String tok) {
        if (tok == null) return null;
        // Find the last 3 '/' separators from the right.
        int s3 = tok.lastIndexOf('/');
        if (s3 < 0) return null;
        int s2 = tok.lastIndexOf('/', s3 - 1);
        if (s2 < 0) return null;
        int s1 = tok.lastIndexOf('/', s2 - 1);
        if (s1 < 0) return null;

        String word = tok.substring(0, s1);
        String pos = tok.substring(s1 + 1, s2);
        String dep = tok.substring(s2 + 1, s3);
        String headStr = tok.substring(s3 + 1);

        int headIdx;
        try {
            headIdx = Integer.parseInt(headStr);
        } catch (NumberFormatException e) {
            return null;
        }
        return new Token(word, pos, dep, headIdx);
    }
}
