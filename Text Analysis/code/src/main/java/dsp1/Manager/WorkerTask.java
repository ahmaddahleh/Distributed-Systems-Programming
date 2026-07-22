package dsp1.Manager;

import org.json.JSONObject;

public final class WorkerTask {
    private final String taskId;
    private final String subTaskId;
    private final String analysis;
    private final String url;

    public WorkerTask(String taskId, String subTaskId, String analysis, String url) {
        this.taskId = taskId;
        this.subTaskId = subTaskId;
        this.analysis = analysis;
        this.url = url;
    }

    public String taskId() {
        return taskId;
    }

    public String subTaskId() {
        return subTaskId;
    }

    public String analysis() {
        return analysis;
    }

    public String url() {
        return url;
    }

    public JSONObject toJson() {
        JSONObject task = new JSONObject();
        task.put("type", "workerTask");
        task.put("taskId", taskId);
        task.put("subTaskId", subTaskId);
        task.put("analysis", analysis);
        task.put("url", url);
        return task;
    }
}
