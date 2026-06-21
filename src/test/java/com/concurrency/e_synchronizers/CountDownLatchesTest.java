package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static org.junit.jupiter.api.Assertions.*;

class CountDownLatchesTest {

    // Isolate our test execution tracking state away from external static variables
    private static int totalSeats = 2;
    private static final Lock testLock = new ReentrantLock();

    @BeforeEach
    void resetState() {
        totalSeats = 2;
    }

    @Test
    @DisplayName("Scenario 1: Verify Partial Latch Countdown Blocks Execution (Boundary Counter > 0)")
    void testPartialCountdownKeepsThreadsBlocked() throws InterruptedException {
        CountDownLatch testLatch = new CountDownLatch(3);

        TripPlannerWithCountDownLatch adderThread = new TripPlannerWithCountDownLatch("Adder-Test-Node", testLatch);
        adderThread.start();

        // Let the thread scheduler catch up and park the thread at await()
        adderThread.waitForThreadToStabilize(adderThread);

        // Fire off 2 out of 3 countdowns
        testLatch.countDown();
        testLatch.countDown();

        // The latch is at 1, the thread MUST remain WAITING
        assertEquals(1, testLatch.getCount());
        assertEquals(Thread.State.WAITING, adderThread.getState(),
                "The thread breached the gate while latch countdown was still greater than zero!");

        // Clean up latch to let thread exit
        testLatch.countDown();
        adderThread.join(1000);
    }

    @Test
    @DisplayName("Scenario 2: Zero Capacity Boundary - Verifies all threads unlock instantly when latch drops to 0")
    void testZeroBoundaryUnlocksAllThreadsSimultaneously() throws InterruptedException {
        CountDownLatch testLatch = new CountDownLatch(3);
        int totalWorkers = 4;
        TripPlannerWithCountDownLatch[] workerCluster = new TripPlannerWithCountDownLatch[totalWorkers];

        for (int i = 0; i < totalWorkers; i++) {
            workerCluster[i] = new TripPlannerWithCountDownLatch("Adder-Node-" + i, testLatch);
            workerCluster[i].start();
        }

        // Ensure every single thread is completely bottle necked at the latch before proceeding
        for (TripPlannerWithCountDownLatch planner : workerCluster) {
            planner.waitForThreadToStabilize(planner);
            assertEquals(Thread.State.WAITING, planner.getState(),
                    "Thread " + planner.getName() + " failed to stabilize inside the await blocker.");
        }

        // Melt the latch down to 0
        testLatch.countDown();
        testLatch.countDown();
        testLatch.countDown();

        assertEquals(0, testLatch.getCount());

        // Now they should clean up and close without hanging
        for (TripPlannerWithCountDownLatch planner : workerCluster) {
            planner.join(1000);
            assertFalse(planner.isAlive(), "Thread failed to finalize execution after latch reached 0.");
        }
    }

    @Test
    @DisplayName("Scenario 3: Interruption Vulnerability Boundary - Thread handles premature control context cancellation cleanly")
    void testPlannerThreadHandlesEmergencyInterruptionGracefully() throws InterruptedException {
        CountDownLatch persistentLatch = new CountDownLatch(1);
        TripPlannerWithCountDownLatch lockedThread = new TripPlannerWithCountDownLatch("Multiplier-Interrupt-Node", persistentLatch);

        lockedThread.start();
        lockedThread.waitForThreadToStabilize(lockedThread);

        // Zap it while it's waiting inside await()
        lockedThread.interrupt();

        lockedThread.join(1000);
        assertFalse(lockedThread.isAlive(), "Thread stayed alive after an explicit interruption signal.");
    }
}