package dsp1.Manager;

import dsp1.RuntimeConfig;

import java.time.Duration;

public final class DurableManagerConfig {
    private final String managerId;
    private final Duration leaseDuration;
    private final Duration staleDispatchTimeout;
    private final Duration recoveryInterval;

    public DurableManagerConfig(String managerId,
            Duration leaseDuration,
            Duration staleDispatchTimeout,
            Duration recoveryInterval) {
        this.managerId = managerId;
        this.leaseDuration = leaseDuration;
        this.staleDispatchTimeout = staleDispatchTimeout;
        this.recoveryInterval = recoveryInterval;
    }

    public static DurableManagerConfig fromRuntime() {
        return new DurableManagerConfig(
                RuntimeConfig.managerId(),
                Duration.ofSeconds(RuntimeConfig.leaseDurationSeconds()),
                Duration.ofSeconds(RuntimeConfig.staleDispatchSeconds()),
                Duration.ofSeconds(RuntimeConfig.recoveryIntervalSeconds()));
    }

    public String managerId() {
        return managerId;
    }

    public Duration leaseDuration() {
        return leaseDuration;
    }

    public Duration staleDispatchTimeout() {
        return staleDispatchTimeout;
    }

    public Duration recoveryInterval() {
        return recoveryInterval;
    }
}
