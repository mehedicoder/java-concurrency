package com.concurrency.a_thread_process;

import java.util.concurrent.TimeUnit;

/**
 * A low-priority telemetry task designed to run continuously in the background.
 * It tracks JVM memory status every second.
 */
class SystemHealthMonitor implements Runnable {

    @Override
    public void run() {
        // Infinite loops are safe inside daemon threads because the JVM
        // will forcefully terminate them when main threads finish.
        while (true) {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

            System.out.format("[DAEMON-MONITOR] Live JVM Memory Usage: %d MB\n", usedMemory);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.out.println("[DAEMON-MONITOR] Telemetry interrupted during shutdown process.");
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

public class E_DaemonThread {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[SERVER-ENGINE] Booting main web server application...");

        // 1. Create the worker thread using our Runnable
        Thread monitorThread = new Thread(new SystemHealthMonitor(), "JVM-Telemetry-Worker");

        // 2. MARK AS DAEMON
        // Must be called BEFORE starting the thread, or it throws an IllegalThreadStateException.
        monitorThread.setDaemon(true);
        monitorThread.start();

        // 3. Simulate the main server handling incoming user web requests
        for (int i = 1; i <= 3; i++) {
            System.out.format("[SERVER-ENGINE] Processing batch #%d of active HTTP requests...\n", i);
            TimeUnit.MILLISECONDS.sleep(800);
        }

        // 4. Server natural termination
        System.out.println("[SERVER-ENGINE] Received graceful shutdown command. Stopping traffic...");
        System.out.println("[SERVER-ENGINE] Core engine has terminated successfully.");

        // At this exact point, since 'monitorThread' is a Daemon, the application exits
        // immediately instead of waiting around forever for the monitor's while(true) loop!
    }
}