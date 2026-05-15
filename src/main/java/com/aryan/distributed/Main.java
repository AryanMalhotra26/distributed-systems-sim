package com.aryan.distributed;

import com.aryan.distributed.consensus.ConsensusCluster;
import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.NetworkSimulator;
import com.aryan.distributed.election.BullyElection;
import com.aryan.distributed.election.RingElection;
import com.aryan.distributed.fault.HeartbeatDetector;

import java.time.Duration;
import java.util.List;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        runBullyDemo();
        runRingDemo();
        runPaxosDemo();
        runHeartbeatDemo();
    }

    private static void runBullyDemo() throws InterruptedException {
        System.out.println("=== Bully Election Demo ===");
        List<Integer> ids = List.of(1, 2, 3);
        try (NetworkSimulator simulator = new NetworkSimulator(10, 0.0, 1L); MessageBus bus = new MessageBus(simulator)) {
            List<BullyElection> nodes = ids.stream()
                    .map(id -> new BullyElection(id, bus, ids, 100))
                    .toList();
            nodes.forEach(BullyElection::start);
            nodes.get(0).triggerElection();
            Thread.sleep(250);
            System.out.println("Leader elected: Node " + nodes.get(0).getLeaderId());
            nodes.forEach(BullyElection::shutdown);
        }
    }

    private static void runRingDemo() throws InterruptedException {
        System.out.println("=== Ring Election Demo ===");
        List<Integer> ids = List.of(4, 1, 3, 2);
        try (NetworkSimulator simulator = new NetworkSimulator(5, 0.0, 2L); MessageBus bus = new MessageBus(simulator)) {
            List<RingElection> nodes = ids.stream()
                    .map(id -> new RingElection(id, bus, ids))
                    .toList();
            nodes.forEach(RingElection::start);
            nodes.get(1).triggerElection();
            Thread.sleep(250);
            System.out.println("Ring leader: Node " + nodes.get(2).getLeaderId());
            nodes.forEach(RingElection::shutdown);
        }
    }

    private static void runPaxosDemo() {
        System.out.println("=== Paxos Demo ===");
        try (ConsensusCluster cluster = new ConsensusCluster(5)) {
            String value = cluster.proposeValue(1, "commit-order-42", Duration.ofSeconds(2));
            System.out.println("Committed value: " + value);
        }
    }

    private static void runHeartbeatDemo() throws InterruptedException {
        System.out.println("=== Heartbeat Detector Demo ===");
        try (HeartbeatDetector detector = new HeartbeatDetector(150, 25)) {
            detector.recordHeartbeat(10);
            Thread.sleep(60);
            detector.recordHeartbeat(10);
            Thread.sleep(220);
            System.out.println("Node 10 suspected: " + detector.isSuspected(10));
        }
    }
}
