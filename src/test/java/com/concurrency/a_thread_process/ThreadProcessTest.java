package com.concurrency.a_thread_process;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Log Analysis Task Specification")
class ThreadProcessTest {

    @Test
    @DisplayName("Should detect exactly 10 errors during a standard synchronous production simulation")
    void testProductionConstructorSimulatedExecution() {
        // Given: Instantiate using the production-facing constructor
        LogAnalysisTask task = new LogAnalysisTask("production_simulation.log");

        // When: Run it directly on the current thread (simulating a full synchronous cycle)
        task.run();

        // Then: Based on the code logic (loop of 10M, matching every 1M iterations),
        // it should hit exactly 10 errors.
        assertEquals(10, task.getErrorsFound(),
                "The production simulation loop should register exactly 10 errors.");
    }

    @Test
    @DisplayName("Should process multiple tasks concurrently using a multi-threaded executor pool")
    void testMultiThreadedExecutionWithProductionConstructor() throws InterruptedException {
        // Given: Create multiple tasks using the requested constructor
        LogAnalysisTask task1 = new LogAnalysisTask("live_server_A.log");
        LogAnalysisTask task2 = new LogAnalysisTask("live_server_B.log");

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // When: Submitting tasks to be processed by background worker threads
        executor.submit(task1);
        executor.submit(task2);

        // Properly await termination of async processing
        executor.shutdown();
        boolean finishedCleanly = executor.awaitTermination(5, TimeUnit.SECONDS);

        // Then: Both threads should complete within a reasonable window and calculate results
        assertTrue(finishedCleanly, "The background executor tasks took too long or hung.");
        assertEquals(10, task1.getErrorsFound(), "Task 1 should have counted 10 errors.");
        assertEquals(10, task2.getErrorsFound(), "Task 2 should have counted 10 errors.");
    }

    @Test
    @DisplayName("Should block and correctly aggregate metrics when a thread join is invoked")
    void joinWaitsUntilTaskFinishes() throws InterruptedException {
        LogAnalysisTask task = new LogAnalysisTask("test.log", List.of(
                "2026-06-13 16:42:00 [ERROR] Connection timed out",
                "2026-06-13 16:43:00 [INFO] Connection established",
                "2026-06-13 16:44:00 [ERROR] Failed to retrieve data",
                "2026-06-13 16:45:00 [WARN] High latency detected",
                "2026-06-13 16:46:00 [ERROR] Database connection lost"
        ));
        Thread thread = new Thread(task);
        thread.start();
        thread.join();

        assertFalse(thread.isAlive());
        assertEquals(3, task.getErrorsFound());
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when required log file parameters are missing")
    void rejectsNegativeIterationCount() {
        assertThrows(IllegalArgumentException.class,
                () -> new LogAnalysisTask(null, null),
                "Initialization should fail if critical parameters are null");
    }
}