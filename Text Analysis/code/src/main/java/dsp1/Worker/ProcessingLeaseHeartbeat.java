package dsp1.Worker;

import dsp1.persistence.ProcessingLeaseRenewalStatus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ProcessingLeaseHeartbeat implements WorkerMessageHandler.LeaseHeartbeat {
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> task;
    private final AtomicBoolean ownershipLost;

    private ProcessingLeaseHeartbeat(ScheduledExecutorService scheduler, ScheduledFuture<?> task,
            AtomicBoolean ownershipLost) {
        this.scheduler = scheduler;
        this.task = task;
        this.ownershipLost = ownershipLost;
    }

    public static ProcessingLeaseHeartbeat start(WorkerTaskRequest request,
            WorkerMessageHandler.ProcessingLeaseCoordinator coordinator,
            long heartbeatIntervalMillis) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "worker-processing-lease-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        AtomicBoolean ownershipLost = new AtomicBoolean(false);
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            try {
                ProcessingLeaseRenewalStatus status = coordinator.renew(request);
                if (status != ProcessingLeaseRenewalStatus.RENEWED) {
                    ownershipLost.set(true);
                }
            } catch (RuntimeException e) {
                ownershipLost.set(true);
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        return new ProcessingLeaseHeartbeat(scheduler, task, ownershipLost);
    }

    @Override
    public boolean ownershipLost() {
        return ownershipLost.get();
    }

    @Override
    public void close() {
        task.cancel(false);
        scheduler.shutdownNow();
    }
}
