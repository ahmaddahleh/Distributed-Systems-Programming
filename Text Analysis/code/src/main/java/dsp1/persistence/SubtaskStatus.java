package dsp1.persistence;

public enum SubtaskStatus {
    PENDING,
    DISPATCHED,
    SUCCEEDED,
    FAILED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED;
    }
}
