package com.aryan.distributed.consensus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaxosTest {

    @Test
    void reachesConsensusAcrossCluster() {
        try (ConsensusCluster cluster = new ConsensusCluster(5)) {
            String committed = cluster.proposeValue(1, "alpha", Duration.ofSeconds(2));
            assertEquals("alpha", committed);
            cluster.getNodes().forEach(node -> assertEquals("alpha", node.getCommittedValue()));
        }
    }

    @Test
    void concurrentProposalsConvergeOnSingleValue() throws Exception {
        try (ConsensusCluster cluster = new ConsensusCluster(5);
             var executor = Executors.newFixedThreadPool(2)) {
            CompletableFuture<String> first = CompletableFuture.supplyAsync(
                    () -> cluster.proposeValue(1, "alpha", Duration.ofSeconds(2)),
                    executor
            );
            CompletableFuture<String> second = CompletableFuture.supplyAsync(
                    () -> cluster.proposeValue(2, "beta", Duration.ofSeconds(2)),
                    executor
            );

            String firstResult = first.get(3, TimeUnit.SECONDS);
            String secondResult = second.get(3, TimeUnit.SECONDS);
            Set<String> allowedValues = Set.of("alpha", "beta");

            assertEquals(firstResult, secondResult);
            assertTrue(allowedValues.contains(firstResult));
            cluster.getNodes().forEach(node -> assertEquals(firstResult, node.getCommittedValue()));
        }
    }

    @Test
    void failsWhenQuorumIsLost() {
        try (ConsensusCluster cluster = new ConsensusCluster(5)) {
            cluster.getNode(3).shutdown();
            cluster.getNode(4).shutdown();
            cluster.getNode(5).shutdown();

            assertThrows(IllegalStateException.class,
                    () -> cluster.proposeValue(1, "gamma", Duration.ofMillis(400)));
        }
    }
}
