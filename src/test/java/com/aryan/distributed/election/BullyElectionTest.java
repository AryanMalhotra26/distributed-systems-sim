package com.aryan.distributed.election;

import com.aryan.distributed.core.MessageBus;
import com.aryan.distributed.core.NetworkSimulator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BullyElectionTest {

    @Test
    void electsHighestActiveNode() {
        List<Integer> ids = List.of(1, 2, 3, 4);
        try (NetworkSimulator simulator = new NetworkSimulator(5, 0.0, 11L); MessageBus bus = new MessageBus(simulator)) {
            List<BullyElection> nodes = ids.stream()
                    .map(id -> new BullyElection(id, bus, ids, 75))
                    .toList();
            nodes.forEach(BullyElection::start);

            nodes.get(0).triggerElection();
            waitUntil(Duration.ofSeconds(2), () -> nodes.stream().allMatch(node -> node.getLeaderId() == 4));

            nodes.forEach(node -> assertEquals(4, node.getLeaderId()));
            nodes.forEach(BullyElection::shutdown);
        }
    }

    @Test
    void reelectsWhenLeaderStops() {
        List<Integer> ids = List.of(1, 2, 3, 4);
        try (NetworkSimulator simulator = new NetworkSimulator(5, 0.0, 12L); MessageBus bus = new MessageBus(simulator)) {
            List<BullyElection> nodes = ids.stream()
                    .map(id -> new BullyElection(id, bus, ids, 75))
                    .toList();
            nodes.forEach(BullyElection::start);
            nodes.get(0).triggerElection();
            waitUntil(Duration.ofSeconds(2), () -> nodes.stream().allMatch(node -> node.getLeaderId() == 4));

            nodes.get(3).shutdown();
            nodes.get(0).triggerElection();
            waitUntil(Duration.ofSeconds(2), () -> nodes.subList(0, 3).stream().allMatch(node -> node.getLeaderId() == 3));

            nodes.subList(0, 3).forEach(node -> assertEquals(3, node.getLeaderId()));
            nodes.subList(0, 3).forEach(BullyElection::shutdown);
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
