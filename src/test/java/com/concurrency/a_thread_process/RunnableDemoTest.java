package com.concurrency.a_thread_process;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Cache Initializer Specification")
class RunnableDemoTest {

    @Test
    @DisplayName("Should successfully execute the initialization sequence to completion")
    void testCacheInitializationCompletesSuccessfully() throws InterruptedException {
        // Given: Prepare the task and a dedicated thread context
        CacheInitializer initializer = new CacheInitializer();
        Thread workerThread = new Thread(initializer, "Test-Cache-Worker");

        // When: We start the thread and immediately join it
        long startTime = System.currentTimeMillis();
        workerThread.start();
        workerThread.join(4000); // Wait up to 4 seconds for completion

        long duration = System.currentTimeMillis() - startTime;

        // Then: The thread should have completed its work and cleanly died
        assertFalse(workerThread.isAlive(), "The initializer thread should have finished and closed.");
        assertTrue(duration >= 3000, "The warm-up cycle must intentionally block for at least 3 seconds.");
    }

    @Test
    @DisplayName("Should preserve the interruption status flag if interrupted during runtime")
    void testCacheInitializationHandlesInterruptionCleanly() throws InterruptedException {
        // Given: Spin up the initializer task on a thread
        CacheInitializer initializer = new CacheInitializer();

        // We use a latch to safely track the execution state inside our sub-thread
        CountDownLatch threadFinishedLatch = new CountDownLatch(1);
        final boolean[] wasInterruptedStatusPreserved = {false};

        Thread rogueWorker = new Thread(() -> {
            try {
                initializer.run();
                // Check if the thread restored its interrupted flag inside the catch block
                wasInterruptedStatusPreserved[0] = Thread.currentThread().isInterrupted();
            } finally {
                threadFinishedLatch.countDown();
            }
        });

        // When: We start it, wait a tiny bit, and interrupt it mid-sleep
        rogueWorker.start();
        TimeUnit.MILLISECONDS.sleep(200); // Give the thread a split second to enter the sleep state
        rogueWorker.interrupt();

        // Wait for the thread context to break down safely
        boolean cleanExit = threadFinishedLatch.await(1, TimeUnit.SECONDS);

        // Then: The thread should abort instantly and correctly bubble up the interrupt state
        assertTrue(cleanExit, "The worker thread should have terminated instantly without hanging.");
        assertTrue(wasInterruptedStatusPreserved[0],
                "The task failed to re-interrupt itself! The interrupted status flag was cleared and lost.");
    }

    @Test
    @DisplayName("Should execute inside a safe, predictable time window boundary")
    void testExecutionTimeoutBoundary() {
        CacheInitializer initializer = new CacheInitializer();

        // Assert that the runnable execution doesn't suffer from deadlocks or runaway loops
        // Executes on the local test thread
        assertTimeoutPreemptively(Duration.ofSeconds(4), initializer::run, "The initialization process took longer than the strict 4-second timeout limit.");
    }
}