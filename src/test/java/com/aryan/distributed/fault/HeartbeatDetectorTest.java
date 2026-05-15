package com.aryan.distributed.fault;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatDetectorTest {

    @Test
    void suspectsNodeAfterTimeout() {
        try (HeartbeatDetector detector = new HeartbeatDetector(100, 20)) {
            detector.recordHeartbeat(7);
            waitFor(Duration.ofMillis(180));
            assertTrue(detector.isSuspected(7));
        }
    }

    @Test
    void clearsSuspicionWhenHeartbeatReturns() {
        try (HeartbeatDetector detector = new HeartbeatDetector(100, 20)) {
            detector.recordHeartbeat(9);
            waitFor(Duration.ofMillis(180));
            assertTrue(detector.isSuspected(9));

            detector.recordHeartbeat(9);
            waitFor(Duration.ofMillis(40));
            assertFalse(detector.isSuspected(9));
        }
    }

    @Test
    void avoidsFalsePositivesWithRegularHeartbeats() {
        try (HeartbeatDetector detector = new HeartbeatDetector(120, 20)) {
            detector.recordHeartbeat(11);
            waitFor(Duration.ofMillis(60));
            detector.recordHeartbeat(11);
            waitFor(Duration.ofMillis(60));
            detector.recordHeartbeat(11);
            waitFor(Duration.ofMillis(50));
            assertFalse(detector.isSuspected(11));
        }
    }

    private static void waitFor(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interruptedException);
        }
    }
}
