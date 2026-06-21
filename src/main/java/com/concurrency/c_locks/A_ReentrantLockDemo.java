package com.concurrency.c_locks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

class FulfillmentService {
    // Two separate business metrics being tracked independently
    static int totalReservations = 0;
    static int totalFulfillments = 0;

    // The reentrant lock acting as our thread guard
    private static final ReentrantLock serviceLock = new ReentrantLock();

    public static int getTotalFulfillments() {
        return totalFulfillments;
    }

    public static int getTotalReservations() {
        return totalReservations;
    }

    // Isolated action: Reserves a single item in stock
    public void reserveStock() {
        serviceLock.lock();
        try {
            // Un-comment the line below to see reentrancy in action!
            // When called from fulfillOrder(), hold count will be 2.
            // System.out.println("Current Lock Hold Count: " + serviceLock.getHoldCount());

            totalReservations++;
        } finally {
            serviceLock.unlock(); // Always release in a finally block
        }
    }

    // Compound action: Fulfills order AND triggers stock reservation
    public void fulfillOrder() {
        serviceLock.lock();
        try {
            totalFulfillments++;

            // REENTRANCY HAPPENS HERE: This thread already holds 'serviceLock'.
            // Because it is a ReentrantLock, it is allowed to enter reserveStock() safely.
            reserveStock();

        } finally {
            serviceLock.unlock();
        }
    }
}

public class A_ReentrantLockDemo {
    public static void main(String[] args) {
        FulfillmentService service = new FulfillmentService();
        int ordersPerRegion = 10_000;

        // Using try-with-resources to automatically manage our concurrent traffic
        try (ExecutorService regionPool = Executors.newFixedThreadPool(8)) {

            // Region 1: Processing 10,000 checkout cycles
            regionPool.submit(() -> {
                for (int i = 0; i < ordersPerRegion; i++) {
                    service.reserveStock(); // Explicit standalone reservation
                    service.fulfillOrder(); // Compound fulfillment (which handles its own reservation)
                }
            });

            // Region 2: Processing 10,000 checkout cycles simultaneously
            regionPool.submit(() -> {
                for (int i = 0; i < ordersPerRegion; i++) {
                    service.reserveStock();
                    service.fulfillOrder();
                }
            });

        } // Thread pool automatically blocks, waits, and shuts down here

        // --- Production Audit Output ---
        // Mathematical Expectations:
        // Clicks per region = 10,000. Total Loops = 20,000.
        // totalFulfillments gets hit 1 time per loop = 20,000
        // totalReservations gets hit 2 times per loop (once standalone, once inside fulfillOrder) = 40,000
        System.out.println("====== WAREHOUSE INVENTORY AUDIT ======");
        System.out.println("Expected Fulfillments: 20,000 | Actual: " + FulfillmentService.getTotalFulfillments());
        System.out.println("Expected Reservations: 40,000 | Actual: " + FulfillmentService.getTotalReservations());
    }

    // Helper getters to view the static results safely
    public static int getTotalReservations() { return FulfillmentService.totalReservations; }
    public static int getTotalFulfillments() { return FulfillmentService.totalFulfillments; }
}