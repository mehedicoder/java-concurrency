package com.concurrency.d_liveness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LiveLockTest {

    @BeforeEach
    void resetSharedState() throws Exception {
        // Clear down the static transaction queue using reflection to ensure strict test isolation
        Field callsField = CallRouteProcessor.class.getDeclaredField("outstandingCallsToRoute");
        callsField.setAccessible(true);
        callsField.set(null, 500_000);
    }

    @Test
    @DisplayName("Should process global call queues cleanly to completion without collapsing into an execution freeze")
    void testLivelockResolutionAndThroughputProgress() throws Exception {
        // Arrange
        Lock portSwitchA = new ReentrantLock();
        Lock portSwitchB = new ReentrantLock();
        Lock portSwitchC = new ReentrantLock();

        // Fast-forward the workload to a small, deterministic scale (e.g., 50 calls)
        // This validates the code logic and retry mechanics without burning heavy local CPU cycles
        Field callsField = CallRouteProcessor.class.getDeclaredField("outstandingCallsToRoute");
        callsField.setAccessible(true);
        callsField.set(null, 50);

        Thread routerAlpha = new CallRouteProcessor("Test-Router-Alpha", portSwitchA, portSwitchB);
        Thread routerBeta  = new CallRouteProcessor("Test-Router-Beta", portSwitchB, portSwitchC);
        Thread routerGamma = new CallRouteProcessor("Test-Router-Gamma", portSwitchC, portSwitchA);

        // Act
        routerAlpha.start();
        routerBeta.start();
        routerGamma.start();

        // Assign a maximum safety execution threshold (3 seconds).
        // If a regression breaks the logic and traps the worker clusters in an infinite loop,
        // the test fails and aborts instead of hanging the entire CI/CD pipeline.
        routerAlpha.join(3000);
        routerBeta.join(1000);
        routerGamma.join(1000);

        // Assert
        // If any thread remains stuck in an endless loop dropping/picking up locks, it will still be alive
        assertFalse(routerAlpha.isAlive(), "Catastrophic Livelock Triggered! Router Alpha is stuck in an infinite retry loop.");
        assertFalse(routerBeta.isAlive(), "Catastrophic Livelock Triggered! Router Beta is stuck in an infinite retry loop.");
        assertFalse(routerGamma.isAlive(), "Catastrophic Livelock Triggered! Router Gamma is stuck in an infinite retry loop.");

        // Re-read the final remaining value to prove it reached 0
        int finalCallQueueCount = callsField.getInt(null);
        assertEquals(0, finalCallQueueCount, "The routing engine failed to clear down all outstanding network calls.");
    }
}