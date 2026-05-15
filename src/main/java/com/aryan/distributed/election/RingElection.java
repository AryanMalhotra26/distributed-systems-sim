package com.aryan.distributed.election;

import com.aryan.distributed.core.Message;
import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.Node;

import java.util.List;
import java.util.Map;

public class RingElection extends Node {
    private final List<Integer> ringOrder;
    private volatile boolean participant;
    private volatile int leaderId = -1;

    public RingElection(int id, MessageBus messageBus, List<Integer> ringOrder) {
        super(id, messageBus);
        this.ringOrder = List.copyOf(ringOrder);
    }

    public void triggerElection() {
        if (!isRunning()) {
            return;
        }
        participant = true;
        forwardElectionCandidate(getId());
    }

    public int getLeaderId() {
        return leaderId;
    }

    @Override
    protected void handleMessage(Message message) {
        switch (message.getType()) {
            case RING_ELECTION -> handleElectionMessage(message);
            case RING_COORDINATOR -> handleCoordinatorMessage(message);
            default -> {
            }
        }
    }

    private void handleElectionMessage(Message message) {
        int candidateId = message.payloadAsInt("candidateId", message.getFrom());
        if (candidateId == getId()) {
            participant = false;
            leaderId = getId();
            forwardCoordinator(getId());
            return;
        }
        if (candidateId > getId()) {
            participant = true;
            forwardElectionCandidate(candidateId);
            return;
        }
        if (!participant) {
            participant = true;
            forwardElectionCandidate(getId());
        }
    }

    private void handleCoordinatorMessage(Message message) {
        int announcedLeader = message.payloadAsInt("leaderId", message.getFrom());
        if (announcedLeader == getId() && leaderId == getId() && message.getFrom() != getId()) {
            participant = false;
            return;
        }
        leaderId = announcedLeader;
        participant = false;
        if (announcedLeader != getId()) {
            forwardCoordinator(announcedLeader);
        }
    }

    private void forwardElectionCandidate(int candidateId) {
        int nextNode = nextActiveNode();
        send(new Message(Message.Type.RING_ELECTION, getId(), nextNode, Map.of("candidateId", candidateId)));
    }

    private void forwardCoordinator(int electedLeader) {
        int nextNode = nextActiveNode();
        send(new Message(Message.Type.RING_COORDINATOR, getId(), nextNode, Map.of("leaderId", electedLeader)));
    }

    private int nextActiveNode() {
        int currentIndex = ringOrder.indexOf(getId());
        if (currentIndex < 0) {
            throw new IllegalStateException("Node is not part of the ring");
        }
        for (int step = 1; step <= ringOrder.size(); step++) {
            int candidate = ringOrder.get((currentIndex + step) % ringOrder.size());
            if (getMessageBus().isRegistered(candidate)) {
                return candidate;
            }
        }
        return getId();
    }
}
