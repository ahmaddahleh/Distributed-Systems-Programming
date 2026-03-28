package edu.bgu.dirt.model;

public class Token {
    public final String word;
    public final String pos;
    public final String dep;
    public final int headIndex; // 0=root; else 1..n (1-based)

    public Token(String word, String pos, String dep, int headIndex) {
        this.word = word;
        this.pos = pos;
        this.dep = dep;
        this.headIndex = headIndex;
    }

    @Override
    public String toString() { return word + "/" + pos + "/" + dep + "/" + headIndex; }
}
