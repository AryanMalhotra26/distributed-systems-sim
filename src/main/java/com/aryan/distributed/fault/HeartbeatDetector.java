package com.aryan.distributed.fault;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HeartbeatDetector implements AutoCloseable {
    private final long timeoutMillis;
    private final Map<Integer, Long> lastHeartbeatTimes = new ConcurrentHashMap<>();
    private final Set<Integer> suspectedNodes = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;

    public HeartbeatDetector(long timeoutMillis, long checkIntervalMillis) {
        this.timeoutMillis = timeoutMillis;
        AtomicInteger threadIds = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "heartbeat-detector-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.scheduler.scheduleAtFixedRate(this::scanForFailures, checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void recordHeartbeat(int nodeId) {
        lastHeartbeatTimes.put(nodeId, System.currentTimeMillis());
        suspectedNodes.remove(nodeId);
    }

    public boolean isSuspected(int nodeId) {
        return suspectedNodes.contains(nodeId);
    }

    public Set<Integer> getSuspectedNodes() {
        return Set.copyOf(suspectedNodes);
    }

    private void scanForFailures() {
        long now = System.currentTimeMillis();
        lastHeartbeatTimes.forEach((nodeId, lastHeartbeat) -> {
            if (now - lastHeartbeat > timeoutMillis) {
                suspectedNodes.add(nodeId);
            }
        });
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
