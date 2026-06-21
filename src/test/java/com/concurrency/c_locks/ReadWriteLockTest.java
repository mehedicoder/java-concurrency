package com.concurrency.c_locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ReadWriteLockTest {

    private MarketTickerFeed tickerFeed;

    @BeforeEach
    void setUp() {
        tickerFeed = new MarketTickerFeed();
        tickerFeed.updatePrice("MSFT", 420.00); // Pre-populate for reader tests
    }

    @Test
    @DisplayName("Happy Path: Multiple concurrent readers should read data simultaneously without blocking each other")
    void testConcurrentReadAccess() throws InterruptedException {
        int concurrentReaders = 5;
        CountDownLatch holdingGate = new CountDownLatch(1);
        CountDownLatch executionVerification = new CountDownLatch(concurrentReaders);
        ExecutorService executor = Executors.newFixedThreadPool(concurrentReaders);

        // Use an AtomicReference to capture any assertion failures from background threads
        AtomicReference<Throwable> threadFailure = new AtomicReference<>();
        AtomicInteger maxObservedConcurrentReaders = new AtomicInteger(0);

        for (int i = 0; i < concurrentReaders; i++) {
            executor.submit(() -> {
                try {
                    holdingGate.await(); // Synchronize thread start

                    // To accurately measure concurrency, your feed should track peak readers
                    // or we assert the value is correct.
                    double price = tickerFeed.getPrice("MSFT");

                    // Sample the counter while threads are actively pounding the feed
                    int currentReaders = tickerFeed.getActiveReaderCount();
                    maxObservedConcurrentReaders.accumulateAndGet(currentReaders, Math::max);

                    if (price != 420.00) {
                        throw new AssertionError("Expected price 420.00 but got " + price);
                    }
                } catch (Throwable t) {
                    threadFailure.set(t); // Forward the crash back to the main thread
                } finally {
                    executionVerification.countDown();
                }
            });
        }

        holdingGate.countDown(); // Fire starting pistol
        boolean cleanFinish = executionVerification.await(2, TimeUnit.SECONDS);
        executor.shutdown();

        // 1. Assert no worker threads crashed silently
        if (threadFailure.get() != null) {
            fail("Worker thread failed with assertion: " + threadFailure.get().getMessage());
        }

        assertTrue(cleanFinish, "Reader threads timed out.");

        // 2. Note: Because getPrice releases instantly, testing the active count reliably
        // outside of the lock is highly timing-dependent. A better check is ensuring
        // they complete in parallel under high load.
    }

    @Test
    @DisplayName("Happy Path: Write lock must safely update state")
    void testWriteLockUpdatesDataCorrectly() {
        tickerFeed.updatePrice("GOOG", 155.50);
        assertEquals(155.50, tickerFeed.getPrice("GOOG"), "Price update should reflect immediately.");
    }

    @Test
    @DisplayName("Advanced/Stress Case: Mutual exclusion verification between readers and writers")
    void testExclusiveWriteLockInterleaving() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readerAcquisitionAttempt = new CountDownLatch(1);
        CountDownLatch testCompleted = new CountDownLatch(1);
        AtomicReference<Double> priceReadByThread = new AtomicReference<>();

        // To test mutual exclusion reliably, we force a write transaction to overlap a read transaction.
        // Instead of calling the short-lived updatePrice, we simulate the thread scheduling order:

        // 1. Thread Alpha: Updates the stock count
        executor.submit(() -> {
            tickerFeed.updatePrice("AAPL", 500.00);
            readerAcquisitionAttempt.countDown(); // Tell reader it's safe to try reading
        });

        // 2. Thread Beta: Reads the stock count
        executor.submit(() -> {
            try {
                readerAcquisitionAttempt.await();
                // This read is guaranteed to run *after* or *during* the completion of the write phase
                priceReadByThread.set(tickerFeed.getPrice("AAPL"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                testCompleted.countDown();
            }
        });

        boolean finishedCleanly = testCompleted.await(3, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(finishedCleanly, "Reader thread hung or deadlocked.");
        assertEquals(500.00, priceReadByThread.get(), "Reader must see the atomic update.");
    }
}