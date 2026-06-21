package com.concurrency.d_liveness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

class StarvationTest {

    @BeforeEach
    void resetSharedState() throws Exception {
        // Programmatically reset the static unallocatedTradePool using reflection to isolate the test state
        Field poolField = OrderBookStreamer.class.getDeclaredField("unallocatedTradePool");
        poolField.setAccessible(true);
        poolField.set(null, 300_000); // Scaled appropriately for optimal test execution time
    }

    @Test
    @DisplayName("Should demonstrate thread starvation by verifying high statistical variance in work distribution")
    void testThreadStarvationDistributionImbalance() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        // Arrange
        // A standard non-fair lock inherently allows aggressive threads to grab the lock multiple times in a row,
        // which actively causes thread starvation under high contention.
        Lock unfairOrderBookLock = new ReentrantLock(false);

        int clusterNodeCount = 8;
        List<OrderBookStreamer> activeNodes = new ArrayList<>();
        List<Integer> finalWorkDistribution = Collections.synchronizedList(new ArrayList<>());

        // Act: Initialize and spawn our heavy contention thread cluster
        for (int i = 0; i < clusterNodeCount; i++) {
            OrderBookStreamer node = new OrderBookStreamer("HFT-Test-Node-" + i, unfairOrderBookLock) {
                @Override
                public void run() {
                    // Call the original execution tracking code
                    super.run();

                    // Intercept and harvest the internal private transaction result after thread completion
                    try {
                        Field allocationsField = this.getClass().getSuperclass().getDeclaredField("successfulAllocations");
                        allocationsField.setAccessible(true);
                        int processedItems = allocationsField.getInt(this);
                        finalWorkDistribution.add(processedItems);
                    } catch (Exception e) {
                        fail("Failed to extract thread processing telemetry via reflection: " + e.getMessage());
                    }
                }
            };
            activeNodes.add(node);
        }

        // Fire all threads simultaneously
        for (OrderBookStreamer node : activeNodes) {
            node.start();
        }

        // Await thread completion with a strict safety timeout boundary
        for (OrderBookStreamer node : activeNodes) {
            node.join(5000);
            assertFalse(node.isAlive(), "Test node timed out or deadlocked under heavy resource contention.");
        }

        // Assert: Evaluate work distribution balance metrics
        assertEquals(clusterNodeCount, finalWorkDistribution.size(), "Telemetry missed data packets from terminated nodes.");

        // Sort results to extract the absolute maximum and minimum workloads handled
        Collections.sort(finalWorkDistribution);
        int minimumWorkHandled = finalWorkDistribution.get(0);
        int maximumWorkHandled = finalWorkDistribution.get(finalWorkDistribution.size() - 1);

        System.out.println("\n============ STARVATION TEST TELEMETRY ============");
        System.out.println("Work Distribution Array (Sorted): " + finalWorkDistribution);
        System.out.println("Least Productive Thread Allocation: " + minimumWorkHandled);
        System.out.println("Most Productive Thread Allocation:  " + maximumWorkHandled);

        // Under severe starvation conditions, the thread scheduling discrepancy is dramatic.
        // We assert that a data imbalance exists (e.g., the hogging thread does significantly more work than the starved thread).
        assertTrue(maximumWorkHandled > (minimumWorkHandled * 5),
                String.format("Starvation vulnerability failed to manifest. Work distribution was too fair. Max: %d, Min: %d",
                        maximumWorkHandled, minimumWorkHandled));
    }
}