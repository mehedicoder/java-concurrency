package com.concurrency.a_thread_process;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Invoice Generator Lifecycle Specification")
class ThreadLifeCycleTest {

    @Test
    @DisplayName("Should cycle cleanly through Thread states (NEW -> TIMED_WAITING -> TERMINATED)")
    void testThreadLifecycleStates() throws InterruptedException {
        // Given: Create the worker thread context (NEW State)
        InvoiceGenerator generator = new InvoiceGenerator();
        Thread worker = new Thread(generator, "Lifecycle-Test-Worker");

        assertEquals(Thread.State.NEW, worker.getState(),
                "Thread should initially be in the NEW state.");

        // When: We start the thread (RUNNABLE / TIMED_WAITING)
        worker.start();

        // Let it start up and fall into its internal Thread.sleep(3000)
        TimeUnit.MILLISECONDS.sleep(200);

        // Then: It should be trapped waiting for the simulation timer to expire
        assertEquals(Thread.State.TIMED_WAITING, worker.getState(),
                "Worker should be in TIMED_WAITING state while generating the PDF.");

        // When: We wait for the thread to completely finish
        worker.join(4000); // 4-second timeout guard

        // Then: Thread should be successfully destroyed
        assertEquals(Thread.State.TERMINATED, worker.getState(),
                "Thread should be in TERMINATED state after completion.");
    }

    @Test
    @DisplayName("Should immediately abort and restore the interruption flag if cancelled mid-generation")
    void testHandlesInterruptionGracefully() throws InterruptedException {
        // Given: Prepare an execution hook tracking the thread wrapper exit status
        InvoiceGenerator generator = new InvoiceGenerator();
        CountDownLatch executionLatch = new CountDownLatch(1);
        final boolean[] flagRestored = {false};

        Thread asyncWorker = new Thread(() -> {
            try {
                generator.run();
                // Check if the thread successfully restored its flag inside its catch block
                flagRestored[0] = Thread.currentThread().isInterrupted();
            } finally {
                executionLatch.countDown();
            }
        });

        // When: Start and interrupt the process almost immediately
        asyncWorker.start();
        TimeUnit.MILLISECONDS.sleep(150); // Let it transition to the sleep block
        asyncWorker.interrupt();

        // Await the thread's termination up to 1 second
        boolean gracefulAborted = executionLatch.await(1, TimeUnit.SECONDS);

        // Then: Ensure it broke out instantly and safely recorded the interruption
        assertTrue(gracefulAborted, "The invoice thread hung instead of aborting immediately.");
        assertTrue(flagRestored[0], "The Thread interrupted status flag was cleared and lost.");
    }

    @Test
    @DisplayName("Should complete within a predictable processing time ceiling")
    void testPerformanceTimeCeiling() {
        InvoiceGenerator generator = new InvoiceGenerator();

        // Assert that running the task synchronously stays inside expected parameters
        assertTimeoutPreemptively(Duration.ofMillis(3500), () -> {
            generator.run();
        }, "The PDF generator task exceeded the 3.5-second runtime limit threshold.");
    }
}