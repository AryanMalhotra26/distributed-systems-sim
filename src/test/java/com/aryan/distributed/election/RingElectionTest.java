package com.aryan.distributed.election;

import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.NetworkSimulator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RingElectionTest {

    @Test
    void electsHighestIdInRing() {
        List<Integer> ids = List.of(4, 1, 3, 2);
        try (NetworkSimulator simulator = new NetworkSimulator(5, 0.0, 21L); MessageBus bus = new MessageBus(simulator)) {
            List<RingElection> nodes = ids.stream()
                    .map(id -> new RingElection(id, bus, ids))
                    .toList();
            nodes.forEach(RingElection::start);

            nodes.get(1).triggerElection();
            waitUntil(Duration.ofSeconds(2), () -> nodes.stream().allMatch(node -> node.getLeaderId() == 4));

            nodes.forEach(node -> assertEquals(4, node.getLeaderId()));
            nodes.forEach(RingElection::shutdown);
        }
    }

    @Test
    void skipsStoppedNodesInRing() {
        List<Integer> ids = List.of(1, 2, 3, 4);
        try (NetworkSimulator simulator = new NetworkSimulator(5, 0.0, 22L); MessageBus bus = new MessageBus(simulator)) {
            List<RingElection> nodes = ids.stream()
                    .map(id -> new RingElection(id, bus, ids))
                    .toList();
            nodes.forEach(RingElection::start);
            nodes.get(3).shutdown();

            nodes.get(0).triggerElection();
            waitUntil(Duration.ofSeconds(2), () -> nodes.subList(0, 3).stream().allMatch(node -> node.getLeaderId() == 3));

            nodes.subList(0, 3).forEach(node -> assertEquals(3, node.getLeaderId()));
            nodes.subList(0, 3).forEach(RingElection::shutdown);
        }
    }

    private static void waitUntil(Duration timeout, CheckedCondition condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) {
                return;
            }
            sleep(20);
        }
        throw new AssertionError("Condition not satisfied within " + timeout);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interruptedException);
        }
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate();
    }
}
