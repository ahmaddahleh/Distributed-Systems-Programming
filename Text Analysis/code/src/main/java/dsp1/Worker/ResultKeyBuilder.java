package dsp1.Worker;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public final class ResultKeyBuilder {

    private ResultKeyBuilder() {
    }

    public static String build(String taskId, String subTaskId, String sourceUrl, String analysisType) {
        return "results/" + safeSegment(taskId)
                + "/" + safeSegment(subTaskId)
                + "_" + safeSegment(analysisType)
                + "_" + hash(sourceUrl)
                + ".txt";
    }

    public static String localInputFileName(String subTaskId) {
        return safeSegment(subTaskId) + "_inputfile";
    }

    public static String localOutputFileName(String subTaskId) {
        return safeSegment(subTaskId) + "_output.txt";
    }

    private static String safeSegment(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static String hash(String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        int h = 0;
        for (byte b : bytes) {
            h = 31 * h + (b & 0xff);
        }
        return Integer.toHexString(h).toLowerCase(Locale.ROOT);
    }
}
