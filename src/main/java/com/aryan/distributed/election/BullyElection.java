package com.aryan.distributed.election;

import com.aryan.distributed.core.Message;
import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.Node;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BullyElection extends Node {
    private final List<Integer> clusterNodeIds;
    private final long electionTimeoutMillis;
    private final ScheduledExecutorService scheduler;
    private final Object electionLock = new Object();
    private volatile boolean electionInProgress;
    private volatile boolean receivedOk;
    private volatile int leaderId = -1;

    public BullyElection(int id, MessageBus messageBus, Collection<Integer> clusterNodeIds, long electionTimeoutMillis) {
        super(id, messageBus);
        this.clusterNodeIds = List.copyOf(clusterNodeIds);
        this.electionTimeoutMillis = electionTimeoutMillis;
        AtomicInteger threadIds = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "bully-scheduler-" + id + "-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public void triggerElection() {
        synchronized (electionLock) {
            if (!isRunning()) {
                return;
            }
            electionInProgress = true;
            receivedOk = false;
            leaderId = -1;
            List<Integer> higherNodes = clusterNodeIds.stream()
                    .filter(nodeId -> nodeId > getId())
                    .filter(getMessageBus()::isRegistered)
                    .toList();
            if (higherNodes.isEmpty()) {
                becomeLeader();
                return;
            }
            for (Integer nodeId : higherNodes) {
                send(Message.of(Message.Type.ELECTION, getId(), nodeId));
            }
            scheduler.schedule(this::onElectionTimeout, electionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    public int getLeaderId() {
        return leaderId;
    }

    @Override
    protected void handleMessage(Message message) {
        switch (message.getType()) {
            case ELECTION -> handleElectionMessage(message);
            case OK -> handleOkMessage();
            case COORDINATOR -> handleCoordinatorMessage(message);
            default -> {
            }
        }
    }

    private void handleElectionMessage(Message message) {
        send(Message.of(Message.Type.OK, getId(), message.getFrom()));
        synchronized (electionLock) {
            if (!electionInProgress) {
                scheduler.execute(this::triggerElection);
            }
        }
    }

    private void handleOkMessage() {
        synchronized (electionLock) {
            if (!electionInProgress) {
                return;
            }
            receivedOk = true;
            scheduler.schedule(this::onCoordinatorTimeout, electionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void handleCoordinatorMessage(Message message) {
        synchronized (electionLock) {
            leaderId = message.payloadAsInt("leaderId", message.getFrom());
            electionInProgress = false;
            receivedOk = false;
        }
    }

    private void onElectionTimeout() {
        synchronized (electionLock) {
            if (electionInProgress && !receivedOk) {
                becomeLeader();
            }
        }
    }

    private void onCoordinatorTimeout() {
        synchronized (electionLock) {
            if (electionInProgress && receivedOk && leaderId == -1) {
                receivedOk = false;
                scheduler.execute(this::triggerElection);
            }
        }
    }

    private void becomeLeader() {
        leaderId = getId();
        electionInProgress = false;
        receivedOk = false;
        for (Integer nodeId : clusterNodeIds) {
            if (nodeId != getId() && getMessageBus().isRegistered(nodeId)) {
                send(new Message(Message.Type.COORDINATOR, getId(), nodeId, Map.of("leaderId", getId())));
            }
        }
    }

    @Override
    protected void onStop() {
        scheduler.shutdownNow();
    }
}
