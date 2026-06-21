package com.concurrency.e_synchronizers;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TripPlannerWithBarrier extends Thread {

    // Shared global state
    public static int totalPassengerSeats = 2; // Start with a baseline of 2 seats
    private static final Lock masterPlanLock = new ReentrantLock();
    private static final CyclicBarrier barrierPoint = new CyclicBarrier(10); // Synchronize 10 planners

    public TripPlannerWithBarrier(String name) {
        super(name);
    }

    @Override
    public void run() {
        if (this.getName().contains("Adder")) {
            // Adders always expands the group size by adding a fixed 3 extra seats for friends
            masterPlanLock.lock();
            try {
                totalPassengerSeats += 3;
                System.out.format("[%s] ADDED 3 seats. (Current Total: %d)\n",
                        this.getName(), totalPassengerSeats);
            } finally {
                masterPlanLock.unlock();
            }

            try {
                barrierPoint.await();
            } catch (InterruptedException e) {
                System.err.format("[%s] Thread was interrupted during sync.\n", this.getName());
                // Restore the interrupted status so higher-level framework knows
                Thread.currentThread().interrupt();
            } catch (BrokenBarrierException e) {
                System.err.format("[%s] Barrier is broken because another thread failed.\n", this.getName());
                // Take corrective action (e.g., reset barrier, abort task)
            }

        } else { // Multiplier upgrades the booking, doubling whatever the current seat capacity is
            try {
                barrierPoint.await();
            } catch (InterruptedException e) {
                System.err.format("[%s] Thread was interrupted during sync.\n", this.getName());
                // Restore the interrupted status so higher-level framework knows
                Thread.currentThread().interrupt();
            } catch (BrokenBarrierException e) {
                System.err.format("[%s] Barrier is broken because another thread failed.\n", this.getName());
                // Take corrective action (e.g., reset barrier, abort task)
            }

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
}

public class E_RaceConditionScenario2_CyclicBarrier {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Trip Planner Launcher] Constructing group transit pool...\n");

        // Set up 10 planners: 5 Adders and 5 Multipliers
        TripPlannerWithBarrier[] planners = new TripPlannerWithBarrier[10];

        for (int i = 0; i < planners.length / 2; i++) {
            planners[2 * i] = new TripPlannerWithBarrier("Adder-Node-0" + i);
            planners[2 * i + 1] = new TripPlannerWithBarrier("Multiplier-Node-0" + i);
        }

        // Fire off all threads concurrently
        for (TripPlannerWithBarrier planner : planners) {
            planner.start();
        }

        // Await synchronization cleanups
        for (TripPlannerWithBarrier planner : planners) {
            planner.join();
        }

        System.out.println("\n--- Execution Terminated ---");
        System.out.format("Final vehicle size calculated by cluster: %d seats.\n", TripPlannerWithBarrier.totalPassengerSeats);
    }
}
