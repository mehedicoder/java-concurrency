package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaceConditionScenario1Test {

    // =========================================================================
    // TRACK 1 BOUNDARY EDGES: UNSYNCHRONIZED ACTIONS
    // =========================================================================

    @Test
    @DisplayName("Boundary Edge 1.1: Massive Contention Stress - Heavy thread blast maximizes lost updates")
    void testMassiveUnSynchronizedContention() throws InterruptedException {
        FlightReservationEngine engine = new FlightReservationEngine();

        // Edge Configuration: Scale up thread threads to maximize interleaving probability
        int heavyThreadCount = 20;
        int modificationsPerThread = 2000;
        int expectedTotalTarget = heavyThreadCount * modificationsPerThread; // 40,000

        CountDownLatch gate = new CountDownLatch(1);
        List<Thread> cluster = new ArrayList<>();

        Runnable task = () -> {
            try {
                gate.await(); // Hold all threads at the starting line to synchronize impact
                for (int i = 0; i < modificationsPerThread; i++) {
                    engine.incrementBookingCountUnSynchronized();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        for (int i = 0; i < heavyThreadCount; i++) {
            Thread t = new Thread(task);
            cluster.add(t);
            t.start();
        }

        // Release the floodgates simultaneously
        gate.countDown();

        for (Thread t : cluster) {
            t.join(3000);
        }

        System.out.format("[Edge Audit 1.1] Expected Metrics: %d | Actual Tracked Metrics: %d\n",
                expectedTotalTarget, engine.getTotalBookings());

        // Under high contention, the lost updates are statistically guaranteed to skew the total significantly lower
        assertTrue(engine.getTotalBookings() < expectedTotalTarget,
                "Under massive thread saturation, a low-level data race must drop updates.");
    }


    // =========================================================================
    // TRACK 2 BOUNDARY EDGES: HIGH-LEVEL LOGICAL RACES
    // =========================================================================

    @Test
    @DisplayName("Boundary Edge 2.1: Precise Single Seat Limit - Exact 2 threads racing for the final vacancy")
    void testLogicalRaceAtExactSingleSeatBoundary() throws InterruptedException {
        FlightReservationEngine engine = new FlightReservationEngine(); // Initial available seats = 1

        CountDownLatch executionSync = new CountDownLatch(1);
        AtomicInteger successfulBookingsObserved = new AtomicInteger(0);
        AtomicInteger rejectedBookingsObserved = new AtomicInteger(0);

        Runnable purchaseRoute = () -> {
            try {
                executionSync.await(); // Ensure both consumers hit the initial condition at the same CPU clock cycle
                if (engine.isSeatAvailable()) {
                    Thread.sleep(40); // Expose the logical vulnerability gap
                    engine.reserveSeat();
                    successfulBookingsObserved.incrementAndGet();
                } else {
                    rejectedBookingsObserved.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Thread customerAlpha = new Thread(purchaseRoute);
        Thread customerBeta  = new Thread(purchaseRoute);

        customerAlpha.start();
        customerBeta.start();

        executionSync.countDown(); // Fire both concurrent buyers at the lone remaining seat

        customerAlpha.join(1000);
        customerBeta.join(1000);

        // Validation against business constraints:
        // Despite having only 1 seat available, incomplete synchronization allowed BOTH to confirm successfully.
        assertEquals(2, successfulBookingsObserved.get(), "Both threads should have successfully bypassed the separate check step.");
        assertEquals(0, rejectedBookingsObserved.get(), "Neither thread was rejected at the check phase because of the interleaved delay.");
        assertEquals(-1, engine.getAvailableSeats(), "The seat inventory broke the boundary parameters, executing an overbooking down to -1.");
    }

    @Test
    @DisplayName("Boundary Edge 2.2: Saturated Zero Pool - Verify behavior when resource capacity is completely empty")
    void testCheckThenActRaceWhenInventoryIsAlreadyZero() throws InterruptedException {
        FlightReservationEngine engine = new FlightReservationEngine();

        // Drive inventory down to the baseline boundary parameter (0 available seats) before running the test
        engine.reserveSeat();
        assertEquals(0, engine.getAvailableSeats(), "Setup baseline verification failed: seat inventory must start at 0.");

        AtomicInteger overbookedCounter = new AtomicInteger(0);
        Runnable lateBuyerTask = () -> {
            // Even with a gap delay, since the value is already 0, the check should return false immediately
            if (engine.isSeatAvailable()) {
                try { Thread.sleep(30); } catch (InterruptedException ignored) {}
                engine.reserveSeat();
                overbookedCounter.incrementAndGet();
            }
        };

        Thread userA = new Thread(lateBuyerTask);
        Thread userB = new Thread(lateBuyerTask);

        userA.start();
        userB.start();
        userA.join(1000);
        userB.join(1000);

        // Assert: When the state is already zero/negative, a synchronized 'check' evaluates safely,
        // preventing further down-drift data anomalies.
        assertEquals(0, overbookedCounter.get(), "Threads should not be able to pass a check if the inventory is already depleted.");
        assertEquals(0, engine.getAvailableSeats(), "The baseline floor value of 0 was altered incorrectly.");
    }
}