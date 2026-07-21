package dsp1.Manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class JobState {
    private final String taskId;
    private final Set<String> expectedSubTaskIds;
    private final ConcurrentMap<String, WorkerResultRecord> completed = new ConcurrentHashMap<>();
    private final AtomicBoolean finalizing = new AtomicBoolean(false);

    public JobState(String taskId, Collection<String> expectedSubTaskIds) {
        this.taskId = taskId;
        this.expectedSubTaskIds = Set.copyOf(new LinkedHashSet<>(expectedSubTaskIds));
    }

    public static JobState fromTasks(String taskId, Collection<WorkerTask> tasks) {
        List<String> ids = new ArrayList<>();
        for (WorkerTask task : tasks) {
            ids.add(task.subTaskId());
        }
        return new JobState(taskId, ids);
    }

    public String taskId() {
        return taskId;
    }

    public int expectedCount() {
        return expectedSubTaskIds.size();
    }

    public int completedCount() {
        return completed.size();
    }

    public boolean acceptsSubTask(String subTaskId) {
        return expectedSubTaskIds.contains(subTaskId);
    }

    public boolean acceptTerminalResult(WorkerResultRecord record) {
        if (!taskId.equals(record.taskId()) || !acceptsSubTask(record.subTaskId())) {
            return false;
        }
        return completed.putIfAbsent(record.subTaskId(), record) == null;
    }

    public boolean isComplete() {
        return completed.size() == expectedSubTaskIds.size();
    }

    public boolean markFinalizing() {
        return finalizing.compareAndSet(false, true);
    }

    public List<String[]> summaryRows() {
        return completed.values().stream()
                .sorted(Comparator.comparing(WorkerResultRecord::subTaskId))
                .map(WorkerResultRecord::toSummaryRow)
                .toList();
    }
}
