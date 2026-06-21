package com.concurrency.e_synchronizers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TripPlannerWithCountDownLatch extends Thread {

    // Shared global state
    public static int totalPassengerSeats = 2; // Start with a baseline of 2 seats
    private static final Lock masterPlanLock = new ReentrantLock();
    CountDownLatch coreSystemCountDownLatch;

    public TripPlannerWithCountDownLatch(String name, CountDownLatch coreSystemServicesLatch) {
        super(name);
        this.coreSystemCountDownLatch = coreSystemServicesLatch;
    }

    @Override
    public void run() {
        if (this.getName().contains("Adder")) {

            try {
                coreSystemCountDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Adders always expands the group size by adding a fixed 3 extra seats for friends
            masterPlanLock.lock();
            try {
                totalPassengerSeats += 3;
                System.out.format("[%s] ADDED 3 seats. (Current Total: %d)\n",
                        this.getName(), totalPassengerSeats);
            } finally {
                masterPlanLock.unlock();
            }
        } else { // Multiplier upgrades the booking, doubling whatever the current seat capacity is
            masterPlanLock.lock();
            try {
                totalPassengerSeats *= 2;
                System.out.format("[%s] DOUBLED the vehicle capacity. (Current Total: %d)\n",
                        this.getName(), totalPassengerSeats);
            } finally {
                masterPlanLock.unlock();
            }
        }
    }

    void waitForThreadToStabilize(Thread thread) throws InterruptedException {
        int retries = 50;
        while (thread.getState() != Thread.State.WAITING && retries > 0) {
            Thread.sleep(10);
            retries--;
        }
    }
}

public class F_CountDownLatches {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Main System] Booting up transit architecture engine...");

        // MULTI-COUNT LATCH: Requires 3 specific configurations to complete before opening
        CountDownLatch coreSystemServicesLatch = new CountDownLatch(3);

        // Set up 8 planners: 4 Multipliers and 4 Adders
        TripPlannerWithCountDownLatch[] planners = new TripPlannerWithCountDownLatch[8];
        for (int i = 0; i < planners.length / 2; i++) {
            planners[2 * i] = new TripPlannerWithCountDownLatch("Multiplier-Node-0" + i, coreSystemServicesLatch);
            planners[2 * i + 1] = new TripPlannerWithCountDownLatch("Adder-Node-0" + i, coreSystemServicesLatch);
        }

        // Spin up the threads (they start, but immediately block inside run() on await())
        for (TripPlannerWithCountDownLatch planner : planners) {
            planner.start();
        }

        Thread.sleep(800); // Wait a moment to visually show them idling
        System.out.println("\n[Main System] Planners are idling. Executing 3 prerequisite dependencies...");

        // Service 1 Finishes
        Thread.sleep(400);
        System.out.println("[Service 1/3] Database connections pooled successfully.");
        coreSystemServicesLatch.countDown(); // Count drops to 2

        // Service 2 Finishes
        Thread.sleep(400);
        System.out.println("[Service 2/3] Payment gateway handshakes secure.");
        coreSystemServicesLatch.countDown(); // Count drops to 1

        // Service 3 Finishes
        Thread.sleep(400);
        System.out.println("[Service 3/3] Third-party inventory synchronized.");
        System.out.println("\n[Main System] Final constraint resolved. Dropping latch to 0!");

        // This final countdown drops the latch to 0 and instantly triggers the waiting threads simultaneously
        coreSystemServicesLatch.countDown();

        // Await thread finalization
        for (TripPlannerWithCountDownLatch planner : planners) {
            planner.join();
        }

        System.out.println("\n--- Cluster Operations Finalized ---");
        System.out.format("Final calculated vehicle capacity: %d seats.\n", TripPlanner.totalPassengerSeats);
    }
}