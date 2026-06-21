package com.concurrency.e_synchronizers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TripPlanner extends Thread {

    // Shared global state
    public static int totalPassengerSeats = 2; // Start with a baseline of 2 seats
    private static final Lock masterPlanLock = new ReentrantLock();

    public TripPlanner(String name) {
        super(name);
    }

    public TripPlanner(String name, CountDownLatch coreSystemServicesLatch) {
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
}
public class E_RaceConditionScenario2 {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Trip Planner Launcher] Constructing group transit pool...\n");

        // Set up 8 planners: 4 Adders and 4 Multipliers
        TripPlanner[] planners = new TripPlanner[8];

        for (int i = 0; i < planners.length / 2; i++) {
            planners[2 * i] = new TripPlanner("Adder-Node-0" + i);
            planners[2 * i + 1] = new TripPlanner("Multiplier-Node-0" + i);
        }

        // Fire off all threads concurrently
        for (TripPlanner planner : planners) {
            planner.start();
        }

        // Await synchronization cleanups
        for (TripPlanner planner : planners) {
            planner.join();
        }

        System.out.println("\n--- Execution Terminated ---");
        System.out.format("Final vehicle size calculated by cluster: %d seats.\n", TripPlanner.totalPassengerSeats);
        System.out.println("Note: Run this file multiple times. The final seat number changes based on thread ordering.");
    }
}