package com.aryan.distributed.core;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NetworkSimulator implements AutoCloseable {
    private final ScheduledExecutorService scheduler;
    private final Random random;
    private final Map<Integer, Integer> partitionAssignments = new ConcurrentHashMap<>();
    private volatile long fixedDelayMillis;
    private volatile double dropRate;

    public NetworkSimulator() {
        this(0L, 0.0, 7L);
    }

    public NetworkSimulator(long fixedDelayMillis, double dropRate, long seed) {
        this.fixedDelayMillis = fixedDelayMillis;
        this.dropRate = dropRate;
        this.random = new Random(seed);
        AtomicInteger threadIds = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "network-simulator-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newScheduledThreadPool(2, threadFactory);
    }

    public void setFixedDelayMillis(long fixedDelayMillis) {
        this.fixedDelayMillis = Math.max(0L, fixedDelayMillis);
    }

    public void setDropRate(double dropRate) {
        if (dropRate < 0.0 || dropRate > 1.0) {
            throw new IllegalArgumentException("dropRate must be between 0 and 1");
        }
        this.dropRate = dropRate;
    }

    public void createPartition(Collection<Set<Integer>> partitions) {
        partitionAssignments.clear();
        int partitionId = 1;
        for (Set<Integer> partition : partitions) {
            for (Integer nodeId : partition) {
                partitionAssignments.put(nodeId, partitionId);
            }
            partitionId++;
        }
    }

    public void healAllPartitions() {
        partitionAssignments.clear();
    }

    public void route(Message message, Runnable delivery) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(delivery, "delivery");
        if (isPartitioned(message) || shouldDrop()) {
            return;
        }
        scheduler.schedule(delivery, fixedDelayMillis, TimeUnit.MILLISECONDS);
    }

    private boolean shouldDrop() {
        return dropRate > 0.0 && random.nextDouble() < dropRate;
    }

    private boolean isPartitioned(Message message) {
        Integer sourcePartition = partitionAssignments.get(message.getFrom());
        Integer targetPartition = partitionAssignments.get(message.getTo());
        return sourcePartition != null && targetPartition != null && !sourcePartition.equals(targetPartition);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
