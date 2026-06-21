package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class SynchronizedStatementTest {

    private EventInventory inventory;

    @BeforeEach
    void setUp() throws Exception {
        inventory = new EventInventory();

        // CRITICAL REFLECTION WORKAROUND: Because totalSeatsBooked is 'static',
        // we must explicitly force reset it to 0 before every test so tests don't pollute each other.
        Field field = EventInventory.class.getDeclaredField("totalSeatsBooked");
        field.setAccessible(true);
        field.set(null, 0); // Resetting the static primitive int to 0
    }

    @Test
    @DisplayName("Happy Path: Multiple concurrent threads must increment inventory without a single race condition drop")
    void testConcurrentSeatBooking() {
        // Arrange
        int totalThreads = 8;
        int bookingsPerThread = 100_000; // Total expected: 800,000 bookings
        int expectedTotalBookings = totalThreads * bookingsPerThread;

        // Act
        try (ExecutorService threadPool = Executors.newFixedThreadPool(totalThreads)) {
            for (int i = 0; i < totalThreads; i++) {
                final int threadId = i;
                threadPool.submit(() -> {
                    for (int j = 0; j < bookingsPerThread; j++) {
                        inventory.bookSeat("192.168.1." + threadId);
                    }
                });
            }
        } // try-with-resources handles cleanup, ensuring all tasks wrap up before continuing

        // Assert
        assertEquals(expectedTotalBookings, inventory.getTotalSeatsBooked(),
                "The synchronized block should guarantee 100% data integrity under heavy multi-threaded stress.");
    }

    @Test
    @DisplayName("Edge Case: System must verify initial booking counts start exactly at zero")
    void testInitialStateIsZero() {
        // Act & Assert
        assertEquals(0, inventory.getTotalSeatsBooked(),
                "A fresh inventory instance should always report zero total bookings.");
    }

    @Test
    @DisplayName("Edge Case: System must gracefully accept unusual or missing string inputs without blowing up")
    void testUnusualUserIpInputs() {
        // Act & Assert
        assertDoesNotThrow(() -> {
            inventory.bookSeat(null);
            inventory.bookSeat("");
            inventory.bookSeat("INVALID_IP_FORMAT");
        }, "The method background processing logic should tolerate missing/malformed IP strings safely.");

        assertEquals(3, inventory.getTotalSeatsBooked(),
                "The core critical section counter must still run correctly regardless of string parameters.");
    }

    @Test
    @DisplayName("Advanced/Stress Case: Lock Contention Verification via Parallel Thread Interleaving")
    void testTrueInterleavedContention() throws InterruptedException {
        // Arrange
        int threadCount = 2;
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Act
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await(); // Hold all threads at a starting gate
                    for (int j = 0; j < 50_000; j++) {
                        inventory.bookSeat("10.0.0.1");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        latch.countDown(); // Fire starting gun: threads hit the code at the exact same instant
        executor.shutdown();
        boolean finished = executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

        // Assert
        assertTrue(finished, "Execution took too long; potential lock deadlock encountered.");
        assertEquals(100_000, inventory.getTotalSeatsBooked(),
                "Simultaneous execution starting line should still safely total up to exactly 100,000.");
    }
}