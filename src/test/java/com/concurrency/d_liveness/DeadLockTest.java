package com.concurrency.d_liveness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLockTest {

    @BeforeEach
    void resetSharedState() throws Exception {
        // Reset the private static counter using reflection to guarantee test isolation
        Field capacityField = FulfillmentProcessor.class.getDeclaredField("unallocatedStockCapacity");
        capacityField.setAccessible(true);
        capacityField.set(null, 500_000);
    }

    @Test
    @DisplayName("Should detect a circular wait condition and identify a JVM-level deadlock")
    void testFulfillmentProcessorsTriggerDeadlock() throws InterruptedException {
        // Arrange: Recreate the exact three resources from the production scenario
        Lock warehouseInventoryLock = new ReentrantLock();
        Lock accountingLedgerLock = new ReentrantLock();
        Lock shippingCourierAllocationLock = new ReentrantLock();

        // Instantiate the workers with the circular dependency pattern
        Thread workerEast = new FulfillmentProcessor("Test-Worker-US-East", warehouseInventoryLock, accountingLedgerLock);
        Thread workerWest = new FulfillmentProcessor("Test-Worker-EU-West", accountingLedgerLock, shippingCourierAllocationLock);
        Thread workerSouth = new FulfillmentProcessor("Test-Worker-APAC-South", shippingCourierAllocationLock, warehouseInventoryLock);

        // Act: Fire up the concurrent processors
        workerEast.start();
        workerWest.start();
        workerSouth.start();

        // Await a brief safety window to allow the threads to establish their locks and collide
        // Instead of an infinite join(), we wait up to 2 seconds for completion.
        boolean finishedCleanly = false;

        // Let's check if they finish (they shouldn't if they deadlock)
        workerEast.join(500);
        workerWest.join(500);
        workerSouth.join(500);

        // Assert Part 1: Verify the threads are trapped in an infinite lockup state
        assertTrue(workerEast.isAlive(), "Worker East terminated unexpectedly instead of deadlocking.");
        assertTrue(workerWest.isAlive(), "Worker West terminated unexpectedly instead of deadlocking.");
        assertTrue(workerSouth.isAlive(), "Worker South terminated unexpectedly instead of deadlocking.");

        // Assert Part 2: Programmatically query the JVM Thread Diagnostics Tooling (ThreadMXBean)
        // This confirms that it is an actual cryptographic/concurrency deadlock, not just a slow loop.
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] deadlockedThreadIds = threadMXBean.findDeadlockedThreads();

        // Validate that the JVM explicitly caught a circular wait lock chain
        assertNotNull(deadlockedThreadIds, "The JVM failed to register a deadlocked condition loop.");
        assertTrue(deadlockedThreadIds.length >= 3, "Expected at least 3 threads to be trapped in the deadlock chain.");

        // Clean Up Phase (Crucial): Forcefully interrupt and stop the threads so they don't leak
        // and freeze subsequent tests running in your build environment.
        workerEast.interrupt();
        workerWest.interrupt();
        workerSouth.interrupt();

        // Ensure resources are completely cleared down
        workerEast.join(100);
        workerWest.join(100);
        workerSouth.join(100);
    }
}