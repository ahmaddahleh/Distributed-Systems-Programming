package dsp1.Manager;

import java.util.ArrayList;
import java.util.List;

public final class InputTaskParser {

    private InputTaskParser() {
    }

    public static List<WorkerTask> parse(String input, String taskId) {
        List<WorkerTask> tasks = new ArrayList<>();
        String[] lines = input.split("\\r?\\n|\\r");
        int validTaskIndex = 0;

        for (String line : lines) {
            if (line == null || line.trim().isEmpty()) {
                continue;
            }

            String[] parts = line.split("\\t");
            if (parts.length < 2) {
                continue;
            }

            String analysis = parts[0].trim();
            String url = parts[1].trim();
            String subTaskId = taskId + ":" + validTaskIndex;
            tasks.add(new WorkerTask(taskId, subTaskId, analysis, url));
            validTaskIndex++;
        }

        return tasks;
    }
}
