package com.aryan.distributed.consensus;

import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.NetworkSimulator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsensusCluster implements AutoCloseable {
    private final NetworkSimulator networkSimulator;
    private final MessageBus messageBus;
    private final List<PaxosNode> nodes;

    public ConsensusCluster(int nodeCount) {
        this.networkSimulator = new NetworkSimulator();
        this.messageBus = new MessageBus(networkSimulator);
        List<Integer> nodeIds = new ArrayList<>();
        for (int id = 1; id <= nodeCount; id++) {
            nodeIds.add(id);
        }
        this.nodes = nodeIds.stream()
                .map(id -> new PaxosNode(id, messageBus, nodeIds))
                .toList();
        nodes.forEach(PaxosNode::start);
    }

    public List<PaxosNode> getNodes() {
        return nodes;
    }

    public PaxosNode getNode(int nodeId) {
        return nodes.stream()
                .filter(node -> node.getId() == nodeId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown node " + nodeId));
    }

    public String proposeValue(int proposerId, String value, Duration timeout) {
        try {
            return getNode(proposerId).proposeAsync(value).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("Consensus was not reached in time", exception);
        }
    }

    @Override
    public void close() {
        nodes.forEach(PaxosNode::shutdown);
        messageBus.close();
    }
}
