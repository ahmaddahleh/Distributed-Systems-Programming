package edu.bgu.dirt.extract;

public class PathInstance {
    public final String pathId;
    public final String slot;   // X or Y
    public final String filler; // stemmed
    public final long count;

    public PathInstance(String pathId, String slot, String filler, long count) {
        this.pathId = pathId;
        this.slot = slot;
        this.filler = filler;
        this.count = count;
    }
}
