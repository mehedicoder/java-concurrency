package com.concurrency.b_mutual_exclusion;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;

class WarehouseRobot extends Thread {

    // Shared resource: The total inventory stock of a popular item
    private static int totalStockCount = 0;

    // Mutual exclusion lock to ensure only one robot updates the inventory at a time
    private static final Lock inventoryLock = new ReentrantLock();

    private final int itemsToStock;

    public WarehouseRobot(String robotName, int itemsToStock) {
        super(robotName);
        this.itemsToStock = itemsToStock;
    }

    @Override
    public void run() {
        for (int i = 0; i < itemsToStock; i++) {

            // Acquire the lock before modifying shared state
            inventoryLock.lock();
            try {
                totalStockCount++;
                System.out.format("[%s] Successfully logged 1 item. Current shelf stock: %d\n",
                        Thread.currentThread().getName(), totalStockCount);
            } finally {
                // ALWAYS release the lock in a finally block to prevent deadlocks
                inventoryLock.unlock();
            }

            // Simulate the robot driving back to the loading dock to grab the next item
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException e) {
                System.out.format("[%s] Robot operation interrupted.\n", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

public class B_MutualExclusion {
    public static void main(String[] args) throws InterruptedException, IllegalAccessException {
        System.out.println("[Warehouse System] Initializing automated inventory stocking sequence...");

        // Create two robots working simultaneously
        Thread robotAlpha = new WarehouseRobot("Robot-Alpha", 5);
        Thread robotBeta = new WarehouseRobot("Robot-Beta", 5);

        // Start the concurrent operations
        robotAlpha.start();
        robotBeta.start();

        // Wait for both robots to finish their tasks
        robotAlpha.join();
        robotBeta.join();

        System.out.println("[Warehouse System] Stocking sequence complete.");
        // Without mutual exclusion, this number could be lower than 10 due to race conditions
        Field finalCountField = WarehouseRobot.class.getDeclaredFields()[0];
        finalCountField.setAccessible(true);
        System.out.println("[Warehouse System] Final verified inventory count: " + finalCountField.getInt(null));
    }
}