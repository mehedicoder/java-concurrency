package com.concurrency.c_locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ReentrantLockDemoTest {

    private FulfillmentService service;

    @BeforeEach
    void setUp() throws Exception {
        service = new FulfillmentService();

        // RESET STATIC STATE: Because metrics are 'static', we use reflection
        // to reset them to 0 before each test so they do not pollute one another.
        resetStaticField("totalReservations");
        resetStaticField("totalFulfillments");
    }

    private void resetStaticField(String fieldName) throws Exception {
        Field field = FulfillmentService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, 0); // Resets static primitive int to 0
    }

    @Test
    @DisplayName("Happy Path: Multiple concurrent threads must process standalone reservations and compound fulfillments without data loss")
    void testConcurrentOrderProcessing() {
        // Arrange
        int totalThreads = 4;
        int ordersPerThread = 5_000; // Total execution loops = 20,000 across all threads

        // Mathematical expectations:
        // loops * threads = 20,000
        // totalFulfillments gets hit 1x per loop = 20,000
        // totalReservations gets hit 2x per loop (once standalone, once inside fulfillOrder) = 40,000
        int expectedFulfillments = totalThreads * ordersPerThread;
        int expectedReservations = expectedFulfillments * 2;

        // Act
        try (ExecutorService threadPool = Executors.newFixedThreadPool(totalThreads)) {
            for (int i = 0; i < totalThreads; i++) {
                threadPool.submit(() -> {
                    for (int j = 0; j < ordersPerThread; j++) {
                        service.reserveStock();
                        service.fulfillOrder();
                    }
                });
            }
        } // ExecutorService auto-closes and blocks here until all threads finish running completely

        // Assert
        assertEquals(expectedFulfillments, FulfillmentService.getTotalFulfillments(),
                "ReentrantLock must guarantee zero dropped fulfillments.");
        assertEquals(expectedReservations, FulfillmentService.getTotalReservations(),
                "ReentrantLock must guarantee zero dropped reservations during nested lock execution.");
    }

    @Test
    @DisplayName("Edge Case: Initial state for both warehouse metrics must start exactly at zero")
    void testInitialStateIsZero() {
        // Act & Assert
        assertEquals(0, FulfillmentService.getTotalReservations(), "Initial stock reservations must be 0.");
        assertEquals(0, FulfillmentService.getTotalFulfillments(), "Initial order fulfillments must be 0.");
    }

    @Test
    @DisplayName("Advanced/Stress Case: Reentrancy under deep lock contention using a CountDownLatch starter pistol")
    void testLockContentionWithReentrancy() throws InterruptedException {
        // Arrange
        int simultaneousThreads = 2;
        int iterationsPerThread = 10_000;

        int expectedFulfillments = simultaneousThreads * iterationsPerThread; // 20,000
        int expectedReservations = expectedFulfillments * 2;                 // 40,000

        CountDownLatch startingPistol = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(simultaneousThreads);

        // Act
        for (int i = 0; i < simultaneousThreads; i++) {
            executor.submit(() -> {
                try {
                    startingPistol.await(); // Trap all worker threads right here at the starting gate
                    for (int j = 0; j < iterationsPerThread; j++) {
                        service.reserveStock();
                        service.fulfillOrder();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startingPistol.countDown(); // FIRE! Both threads storm the service simultaneously
        executor.shutdown();
        boolean finishedCleanly = executor.awaitTermination(5, TimeUnit.SECONDS);

        // Assert
        assertTrue(finishedCleanly, "The execution timed out or deadlocked! The ReentrantLock failed to unlock.");
        assertEquals(expectedFulfillments, FulfillmentService.getTotalFulfillments(),
                "Fulfillments count is wrong under high lock contention.");
        assertEquals(expectedReservations, FulfillmentService.getTotalReservations(),
                "Reservations count is wrong under high lock contention.");
    }
}