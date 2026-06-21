package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SemaphoresTest {

    @BeforeEach
    void resetSharedStaticSemaphores() throws Exception {
        // Since the semaphores are 'static final', they hold state across test runs.
        // We use reflection to drain or reset them cleanly before each scenario executes.
        resetSemaphoreField("gateSemaphore", 3);
        resetSemaphoreField("fuelingRigSemaphore", 1);
    }

    private void resetSemaphoreField(String fieldName, int permits) throws Exception {
        Field field = Airplane.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        Semaphore semaphore = (Semaphore) field.get(null);

        // Drain any lingering acquired states and release back to standard threshold
        semaphore.drainPermits();
        semaphore.release(permits);
    }

    private Semaphore getSemaphoreInstance(String fieldName) throws Exception {
        Field field = Airplane.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Semaphore) field.get(null);
    }

    @Test
    @DisplayName("Scenario 1: Verify Counting Semaphore Cap (Max 3 Planes Concurrent at Gates)")
    void testCountingSemaphoreGateLimit() throws Exception {
        Semaphore gateSemaphore = getSemaphoreInstance("gateSemaphore");

        // Block the fueling rig completely so planes get stuck inside the gate
        // after landing, allowing us to accurately count them.
        Semaphore fuelingRig = getSemaphoreInstance("fuelingRigSemaphore");
        fuelingRig.drainPermits(); // 0 permits remaining on fueling rig

        // Spawn 5 planes targeting the 3 gates
        Thread[] planes = new Thread[5];
        for (int i = 0; i < 5; i++) {
            planes[i] = new Airplane("TestFlight-GateCheck-" + i);
            planes[i].start();
        }

        // Give the JVM thread scheduler a brief window to route the threads
        Thread.sleep(300);

        // Assert: 3 planes should have successfully grabbed gates, leaving exactly 0 permits.
        // The remaining 2 planes must be stuck waiting outside.
        assertEquals(0, gateSemaphore.availablePermits(),
                "The gate semaphore did not exhaust its permits down to 0 under load.");

        int waitingPlanesCount = 0;
        for (Thread plane : planes) {
            if (plane.getState() == Thread.State.WAITING) {
                waitingPlanesCount++;
            }
        }

        // 2 planes must be actively waiting for a gate permit to release
        assertEquals(2, waitingPlanesCount,
                "The counting semaphore allowed too many threads past the boundary!");

        // Clean up: Release the fueling rig to let threads finish gracefully
        fuelingRig.release(1);
        for (Thread plane : planes) {
            plane.join(2000);
        }
    }

    @Test
    @DisplayName("Scenario 2: Verify Binary Semaphore Mutex (Exactly 1 Plane at Fueling Rig)")
    void testBinarySemaphoreFuelingRigMutex() throws Exception {
        Semaphore gateSemaphore = getSemaphoreInstance("gateSemaphore");
        Semaphore fuelingRigSemaphore = getSemaphoreInstance("fuelingRigSemaphore");

        // Forcefully consume the single fueling permit from the test runner context
        fuelingRigSemaphore.drainPermits();

        // Launch an airplane thread
        Thread plane = new Airplane("TestFlight-MutexCheck");
        plane.start();

        // Allow the plane to slip cleanly past the gate pool (since gates are fully open at 3)
        Thread.sleep(1200); // Exceeds the 1000ms gate sleep in your Airplane source code

        // Assert: The airplane passed the gate but is now trapped waiting for the fueling rig mutex
        assertEquals(Thread.State.WAITING, plane.getState(),
                "The plane thread did not block at the binary semaphore when permits were exhausted.");
        assertEquals(0, fuelingRigSemaphore.availablePermits(),
                "The binary semaphore shouldn't have any permits left.");

        // Clean up: Release the permit to let the plane finish its flight loop
        fuelingRigSemaphore.release();
        plane.join(2000);
    }

    @Test
    @DisplayName("Scenario 3: Interruption Resiliency - Airplane thread terminates cleanly on emergency interrupt")
    void testAirplaneInterruptHandling() throws InterruptedException {
        // Exhaust the gate permits so a new airplane thread immediately blocks on entry
        Airplane p1 = new Airplane("Blocker-1");
        Airplane p2 = new Airplane("Blocker-2");
        Airplane p3 = new Airplane("Blocker-3");
        p1.start(); p2.start(); p3.start();

        Thread.sleep(100); // Ensure they consume all 3 gate positions

        // Create the target test airplane thread
        Thread targetPlane = new Airplane("Interrupted-Flight");
        targetPlane.start();

        Thread.sleep(100); // Ensure it is trapped waiting for a gate

        // Act: Issue an emergency control tower interrupt signal to the waiting thread
        targetPlane.interrupt();
        targetPlane.join(1000);

        // Assert: Thread should have successfully broken out of acquire(), hit catch, and exited cleanly
        assertFalse(targetPlane.isAlive(),
                "The airplane thread failed to shut down or hang-vented when receiving an interrupt signal.");

        // Clean up remaining threads
        p1.interrupt(); p2.interrupt(); p3.interrupt();
        p1.join(1000); p2.join(1000); p3.join(1000);
    }
}