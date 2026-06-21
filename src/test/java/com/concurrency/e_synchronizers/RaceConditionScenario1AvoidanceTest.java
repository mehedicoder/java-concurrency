package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaceConditionScenario1AvoidanceTest {

    // =========================================================================
    // TRACK 1 TESTS: COUNTER MUTEX INTEGRITY
    // =========================================================================

    @Test
    @DisplayName("Scenario 1.1: Verification of Safe Metrics Counter under High Contention")
    void testSynchronizedCounterMaintainsAbsoluteIntegrity() throws InterruptedException {
        // Arrange
        FlightReservationEngineWithCompoundAtomicity engine = new FlightReservationEngineWithCompoundAtomicity();
        int totalThreads = 10;
        int operationsPerThread = 2000;
        int expectedFinalCount = totalThreads * operationsPerThread; // 20,000

        CountDownLatch dynamicStartingLine = new CountDownLatch(1);
        List<Thread> testCluster = new ArrayList<>();

        Runnable countTask = () -> {
            try {
                dynamicStartingLine.await(); // Lockstep synchronization for maximum simultaneous impact
                for (int i = 0; i < operationsPerThread; i++) {
                    engine.incrementBookingCountSynchronized();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        for (int i = 0; i < totalThreads; i++) {
            Thread t = new Thread(countTask);
            testCluster.add(t);
            t.start();
        }

        // Act: Open the gates simultaneously
        dynamicStartingLine.countDown();

        for (Thread t : testCluster) {
            t.join(2000);
        }

        // Assert: Thanks to 'synchronized', no updates can be overwritten or lost
        assertEquals(expectedFinalCount, engine.getTotalBookings(),
                "The metrics counter lost updates! Synchronized block failed to enforce mutual exclusion.");
    }

    // =========================================================================
    // TRACK 2 TESTS: LOGICAL SEAT BOUNDARY EDGES
    // =========================================================================

    @Test
    @DisplayName("Scenario 2.1: Precise Vacancy Boundary - Exact single-seat limit holds up against multiple threads")
    void testCompoundAtomicityPreventsOverbookingAtBoundaryLimit() throws InterruptedException {
        // Arrange: Initial available seats starts at exactly 1
        FlightReservationEngineWithCompoundAtomicity engine = new FlightReservationEngineWithCompoundAtomicity();

        int competingPassengersCount = 8;
        CountDownLatch transactionFloodgates = new CountDownLatch(1);

        AtomicInteger totalConfirmations = new AtomicInteger(0);
        AtomicInteger totalRejections = new AtomicInteger(0);

        Runnable purchaseRoute = () -> {
            try {
                transactionFloodgates.await(); // Synchronize all threads to fire simultaneously
                boolean bookingSuccess = engine.reserveSeat();

                if (bookingSuccess) {
                    totalConfirmations.incrementAndGet();
                } else {
                    totalRejections.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Thread[] consumerThreads = new Thread[competingPassengersCount];
        for (int i = 0; i < competingPassengersCount; i++) {
            consumerThreads[i] = new Thread(purchaseRoute);
            consumerThreads[i].start();
        }

        // Act: Release all threads at once
        transactionFloodgates.countDown();

        for (Thread passenger : consumerThreads) {
            passenger.join(1000);
        }

        // Assert: Because check-then-act is atomic, exactly 1 thread wins, and 7 are rejected
        assertEquals(1, totalConfirmations.get(),
                "Critical Business Failure: Multiple passengers claimed the lone remaining seat!");
        assertEquals(competingPassengersCount - 1, totalRejections.get(),
                "Incorrect number of booking rejections observed.");
        assertEquals(0, engine.getAvailableSeats(),
                "The available seats broke the lower floor boundary of 0!");
    }

    @Test
    @DisplayName("Scenario 2.2: Saturated Floor Boundary - Operations fail predictably when seat pool is empty")
    void testReserveSeatRejectsSafelyWhenInventoryIsPreDepleted() throws InterruptedException {
        // Arrange
        FlightReservationEngineWithCompoundAtomicity engine = new FlightReservationEngineWithCompoundAtomicity();

        // Act: Manually drain the lone seat to push inventory directly to its zero-boundary floor
        boolean firstReservationResult = engine.reserveSeat();
        assertTrue(firstReservationResult, "Setup condition failure: Initial seat reservation should succeed.");
        assertEquals(0, engine.getAvailableSeats(), "Setup validation failure: Seat pool must be exactly 0.");

        // Spawn a trailing late buyer thread targeting the empty pool
        AtomicInteger successCounter = new AtomicInteger(0);
        Runnable lateBuyerTask = () -> {
            if (engine.reserveSeat()) {
                successCounter.incrementAndGet();
            }
        };

        Thread lateBuyer = new Thread(lateBuyerTask);
        lateBuyer.start();
        lateBuyer.join(1000);

        // Assert: It must be rejected, and inventory must remain rock-solid at 0 (never dropping to negative numbers)
        assertEquals(0, successCounter.get(), "A late booking request bypassed the zero boundary rules!");
        assertEquals(0, engine.getAvailableSeats(), "The zero-capacity inventory pool became corrupted.");
    }
}