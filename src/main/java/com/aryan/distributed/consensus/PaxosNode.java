package com.aryan.distributed.consensus;

import com.aryan.distributed.core.Message;
import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.Node;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PaxosNode extends Node {
    private static final AtomicLong GLOBAL_BALLOT = new AtomicLong();

    private final Set<Integer> clusterNodeIds;
    private final ConcurrentMap<Long, ProposalRound> proposalRounds = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, CompletableFuture<String>> proposalResults = new ConcurrentHashMap<>();
    private final Object acceptorLock = new Object();
    private volatile long promisedBallot = -1L;
    private volatile long acceptedBallot = -1L;
    private volatile String acceptedValue;
    private volatile String committedValue;

    public PaxosNode(int id, MessageBus messageBus, Collection<Integer> clusterNodeIds) {
        super(id, messageBus);
        this.clusterNodeIds = Set.copyOf(clusterNodeIds);
    }

    public CompletableFuture<String> proposeAsync(String value) {
        long ballot = GLOBAL_BALLOT.incrementAndGet() * 10 + getId();
        ProposalRound round = new ProposalRound(value, quorumSize());
        proposalRounds.put(ballot, round);
        CompletableFuture<String> result = new CompletableFuture<>();
        proposalResults.put(ballot, result);
        broadcast(Message.Type.PAXOS_PREPARE, Map.of("ballot", ballot, "proposerId", getId()));
        return result;
    }

    public String getCommittedValue() {
        return committedValue;
    }

    @Override
    protected void handleMessage(Message message) {
        switch (message.getType()) {
            case PAXOS_PREPARE -> handlePrepare(message);
            case PAXOS_PROMISE -> handlePromise(message);
            case PAXOS_ACCEPT_REQUEST -> handleAcceptRequest(message);
            case PAXOS_ACCEPTED -> handleAccepted(message);
            case PAXOS_COMMIT -> handleCommit(message);
            default -> {
            }
        }
    }

    private void handlePrepare(Message message) {
        long ballot = message.payloadAsLong("ballot", -1L);
        synchronized (acceptorLock) {
            if (ballot <= promisedBallot) {
                return;
            }
            promisedBallot = ballot;
            send(new Message(Message.Type.PAXOS_PROMISE, getId(), message.getFrom(), Map.of(
                    "ballot", ballot,
                    "acceptedBallot", acceptedBallot,
                    "acceptedValue", acceptedValue == null ? "" : acceptedValue
            )));
        }
    }

    private void handlePromise(Message message) {
        long ballot = message.payloadAsLong("ballot", -1L);
        ProposalRound round = proposalRounds.get(ballot);
        if (round == null || round.acceptPhaseStarted || round.committed) {
            return;
        }
        round.promisedBy.add(message.getFrom());
        long responseAcceptedBallot = message.payloadAsLong("acceptedBallot", -1L);
        String responseAcceptedValue = message.payloadAsString("acceptedValue");
        if (responseAcceptedBallot > round.highestAcceptedBallot && responseAcceptedValue != null && !responseAcceptedValue.isBlank()) {
            round.highestAcceptedBallot = responseAcceptedBallot;
            round.highestAcceptedValue = responseAcceptedValue;
        }
        if (round.promisedBy.size() >= round.quorumSize && !round.acceptPhaseStarted) {
            round.acceptPhaseStarted = true;
            String candidateValue = round.highestAcceptedValue != null ? round.highestAcceptedValue : round.originalValue;
            round.candidateValue = candidateValue;
            broadcast(Message.Type.PAXOS_ACCEPT_REQUEST, Map.of(
                    "ballot", ballot,
                    "value", candidateValue,
                    "proposerId", getId()
            ));
        }
    }

    private void handleAcceptRequest(Message message) {
        long ballot = message.payloadAsLong("ballot", -1L);
        String value = message.payloadAsString("value");
        synchronized (acceptorLock) {
            if (ballot < promisedBallot) {
                return;
            }
            promisedBallot = ballot;
            acceptedBallot = ballot;
            acceptedValue = value;
            send(new Message(Message.Type.PAXOS_ACCEPTED, getId(), message.getFrom(), Map.of(
                    "ballot", ballot,
                    "value", value
            )));
        }
    }

    private void handleAccepted(Message message) {
        long ballot = message.payloadAsLong("ballot", -1L);
        ProposalRound round = proposalRounds.get(ballot);
        if (round == null || round.committed) {
            return;
        }
        round.acceptedBy.add(message.getFrom());
        if (round.acceptedBy.size() >= round.quorumSize) {
            round.committed = true;
            String value = message.payloadAsString("value");
            broadcast(Message.Type.PAXOS_COMMIT, Map.of("ballot", ballot, "value", value));
        }
    }

    private void handleCommit(Message message) {
        String value = message.payloadAsString("value");
        committedValue = value;
        acceptedValue = value;
        proposalResults.values().forEach(result -> result.complete(value));
    }

    private void broadcast(Message.Type type, Map<String, Object> payload) {
        for (Integer nodeId : clusterNodeIds) {
            send(new Message(type, getId(), nodeId, payload));
        }
    }

    private int quorumSize() {
        return (clusterNodeIds.size() / 2) + 1;
    }

    private static final class ProposalRound {
        private final String originalValue;
        private final int quorumSize;
        private final Set<Integer> promisedBy = ConcurrentHashMap.newKeySet();
        private final Set<Integer> acceptedBy = ConcurrentHashMap.newKeySet();
        private volatile long highestAcceptedBallot = -1L;
        private volatile String highestAcceptedValue;
        private volatile String candidateValue;
        private volatile boolean acceptPhaseStarted;
        private volatile boolean committed;

        private ProposalRound(String originalValue, int quorumSize) {
            this.originalValue = originalValue;
            this.quorumSize = quorumSize;
        }
    }
}
