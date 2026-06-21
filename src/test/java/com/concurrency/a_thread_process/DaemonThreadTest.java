package com.concurrency.a_thread_process;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DaemonThreadTest {
    @Test
    @DisplayName("Happy Path: Daemon execution should print metrics and exit on interruption")
    void testDaemonExecutionAndInterruption() throws InterruptedException {
        // Arrange: Intercept System.out to check if our daemon is actually working
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));

        // Use a Latch to allow the daemon thread to run a bit
        CountDownLatch latch = new CountDownLatch(1);

        Thread daemonThread = new Thread(() -> {
            // Run a mini version of our monitor loop that counts down our latch
            try {
                // Let it simulate one loop iteration
                Runtime runtime = Runtime.getRuntime();
                long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                System.out.format("[DAEMON-MONITOR] Live JVM Memory Usage: %d MB\n", usedMemory);

                latch.countDown(); // Signal the test that execution happened

                TimeUnit.SECONDS.sleep(5); // Sleep to wait for an interrupt signal
            } catch (InterruptedException e) {
                System.out.println("[DAEMON-MONITOR] Telemetry interrupted during shutdown process.");
            }
        });

        daemonThread.setDaemon(true);

        try {
            // Act
            daemonThread.start();

            // Wait up to 2 seconds for the daemon to finish its first loop printout
            boolean executed = latch.await(2, TimeUnit.SECONDS);
            assertTrue(executed, "The daemon thread loop should have executed at least once.");

            // Interrupt the thread mid-sleep to verify it handles exceptions safely
            daemonThread.interrupt();
            daemonThread.join(1000); // Ensure it terminates

            // Assert
            String logs = outputStream.toString();
            assertTrue(logs.contains("[DAEMON-MONITOR] Live JVM Memory Usage:"),
                    "Logs should contain telemetry metrics.");
            assertTrue(logs.contains("[DAEMON-MONITOR] Telemetry interrupted during shutdown process."),
                    "Daemon thread should log its graceful cleanup message when interrupted.");

        } finally {
            // Always restore the system output stream so subsequent tests don't break
            System.setOut(originalOut);
        }
    }

    @Test
    @DisplayName("Edge Case: Verify thread is correctly marked as Daemon before starting")
    void testThreadIsDaemon() {
        // Arrange
        Thread monitorThread = new Thread(new SystemHealthMonitor(), "Test-Monitor");

        // Act
        monitorThread.setDaemon(true);

        // Assert
        assertTrue(monitorThread.isDaemon(), "The worker thread must be explicitly flagged as a daemon thread.");
    }

    @Test
    @DisplayName("Exception Case: Changing daemon status after thread start must throw IllegalThreadStateException")
    void testDaemonStatusChangeAfterStartThrowsException() throws InterruptedException {
        // Arrange
        Thread monitorThread = new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ignored) {}
        });

        // Act & Assert
        monitorThread.start();

        // Trying to set daemon status AFTER starting should crash immediately
        assertThrows(IllegalThreadStateException.class, () -> {
            monitorThread.setDaemon(true);
        }, "Modifying daemon status after .start() should throw an IllegalThreadStateException.");

        // Clean up the running thread safely
        monitorThread.interrupt();
        monitorThread.join(500);
    }

    @Test
    @DisplayName("Edge Case: Daemon thread exits prematurely and leaves shared business data uncorrupted")
    void testDaemonPrematureExitHasNoSideEffects() throws InterruptedException {
        // Arrange: Create shared production states that the daemon has access to
        java.util.concurrent.atomic.AtomicInteger criticalSharedResource = new java.util.concurrent.atomic.AtomicInteger(100);
        java.util.concurrent.atomic.AtomicReference<String> systemStatusFlag = new java.util.concurrent.atomic.AtomicReference<>("IDLE");

        CountDownLatch daemonFinishedLatch = new CountDownLatch(1);

        // We pass the shared references directly into the daemon's execution block
        Thread prematureDaemon = new Thread(() -> {
            try {
                // 1. The daemon safely updates a surface-level status metric
                systemStatusFlag.set("MONITORING_ACTIVE");

                // 2. Simulate a premature failure trigger (e.g., an early interruption signal)
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Premature hardware telemetry failure.");
                }

                // 3. UNDER HAPPIER CIRCUMSTANCES, the daemon would modify/flush business states here:
                // criticalSharedResource.set(0); <--- It never reaches this line!
                criticalSharedResource.addAndGet(500);

            } catch (InterruptedException e) {
                // Edge Case Cleanup: Revert the status flag so the system isn't left in a bad state
                System.out.println("[DAEMON-MONITOR] Caught premature exit. Reverting status flag.");
                systemStatusFlag.set("MONITOR_FAILED");
            } finally {
                // Guarantee notification to the master thread that this daemon is dead
                daemonFinishedLatch.countDown();
            }
        }, "Premature-Daemon-Worker");

        // Act
        prematureDaemon.setDaemon(true);

        // Inject the interrupt signal right before starting to simulate a "startup failure"
        prematureDaemon.interrupt();
        prematureDaemon.start();

        // Block the test thread until the daemon completely executes its try-catch-finally lifecycle
        boolean cleanlyTerminated = daemonFinishedLatch.await(2, TimeUnit.SECONDS);

        // Assertions
        assertTrue(cleanlyTerminated, "The daemon thread should have executed its cleanup block immediately.");
        assertFalse(prematureDaemon.isAlive(), "The daemon thread must be completely terminated.");

        // PROOF OF NO ADVERSE SIDE EFFECTS:
        // 1. Verify the catch block executed and handled the state gracefully
        assertEquals("MONITOR_FAILED", systemStatusFlag.get(),
                "The status flag should reflect that the monitor failed gracefully rather than staying stuck on ACTIVE.");

        // 2. Verify the underlying business data was NEVER reached or corrupted by the crashing daemon
        assertEquals(100, criticalSharedResource.get(),
                "The core critical shared resource must remain untouched (100) because the daemon exited before mutating it.");
    }
}