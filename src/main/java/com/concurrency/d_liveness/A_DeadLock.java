package com.concurrency.d_liveness;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class FulfillmentProcessor extends Thread {

    private final Lock primaryResourceLock;
    private final Lock secondaryResourceLock;

    // Shared global metric tracking total items available to fulfill system-wide
    private static int unallocatedStockCapacity = 500_000;

    public FulfillmentProcessor(String workerName, Lock primaryResourceLock, Lock secondaryResourceLock) {
        super(workerName);
        this.primaryResourceLock = primaryResourceLock;
        this.secondaryResourceLock = secondaryResourceLock;
    }

    @Override
    public void run() {
        while (unallocatedStockCapacity > 0) {

            // To process a batch order, the worker MUST lock both independent architectural resources
            primaryResourceLock.lock();
            try {
                // Simulate processing delays, widening the timing window for a deadlock to happen
                Thread.sleep(1);

                secondaryResourceLock.lock();
                try {
                    // Double check state inside the locked critical section
                    if (unallocatedStockCapacity > 0) {
                        unallocatedStockCapacity--;
                        System.out.format("[%s] Successfully verified allocation. Stock remaining: %d\n",
                                this.getName(), unallocatedStockCapacity);
                    }
                } finally {
                    secondaryResourceLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } finally {
                primaryResourceLock.unlock();
            }
        }
    }
}

public class A_DeadLock {
    public static void main(String[] args) {
        System.out.println("[System] Initializing Enterprise Order Fulfillment Engines...");

        // Three shared foundational backend resources requiring mutual exclusion
        Lock warehouseInventoryLock = new ReentrantLock();
        Lock accountingLedgerLock = new ReentrantLock();
        Lock shippingCourierAllocationLock = new ReentrantLock();

        // Worker A locks Inventory, then waits for Accounting
        Thread regionAlphaWorker = new FulfillmentProcessor("Worker-US-East",
                warehouseInventoryLock, accountingLedgerLock);

        // Worker B locks Accounting, then waits for Shipping Courier
        Thread regionBetaWorker = new FulfillmentProcessor("Worker-EU-West",
                accountingLedgerLock, shippingCourierAllocationLock);

        // CRITICAL FLAW (Deadlock Trap): Worker C locks Shipping Courier, then waits for Inventory!
        // This circular wait chain completes a Coffman deadlock condition loop.
        Thread regionGammaWorker = new FulfillmentProcessor("Worker-APAC-South",
                shippingCourierAllocationLock, warehouseInventoryLock);

        // Spin up the threads. Within moments, the application logs will freeze indefinitely.
        regionAlphaWorker.start();
        regionBetaWorker.start();
        regionGammaWorker.start();
    }
}
