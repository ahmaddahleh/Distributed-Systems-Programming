package edu.bgu.dirt.model;

import java.util.List;

public class NgramRecord {
    public final String headWord;
    public final List<Token> tokens;
    public final long totalCount;

    public NgramRecord(String headWord, List<Token> tokens, long totalCount) {
        this.headWord = headWord;
        this.tokens = tokens;
        this.totalCount = totalCount;
    }
}
