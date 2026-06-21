package com.concurrency.a_thread_process;

import java.util.concurrent.TimeUnit;

/**
 * A background initialization task that implements Runnable.
 * This decouples the task execution logic from the Thread lifecycle management.
 */
class CacheInitializer implements Runnable {

    @Override
    public void run() {
        System.out.println("[Cache-Service] Thread started. Fetching configuration matrices...");
        try {
            // Simulate reading 100k rows from database and warming up the local Redis cache
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            System.err.println("[Cache-Service] Critical: Cache warming was interrupted!");
            // Good practice: Restore interrupted status so upper-level code knows about it
            Thread.currentThread().interrupt();
            return;
        }
        System.out.println("[Cache-Service] Cache successfully populated. Ready for low-latency queries.");
    }
}

public class B_RunnableDemo {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[System-Engine] Master boot sequence initiated...");

        // 1. Instantiating a Thread passing our Runnable implementation as a target
        Thread cacheWarmupThread = new Thread(new CacheInitializer(), "Cache-Warmup-Worker");

        System.out.println("[System-Engine] Spawning asynchronous background worker for Cache Warmup...");
        // 2. Start the background thread
        cacheWarmupThread.start();

        System.out.println("[System-Engine] Continuing boot sequence: Loading UI assets and mapping routing controllers...");
        // Simulate local main thread operations taking a brief moment
        TimeUnit.MILLISECONDS.sleep(500);

        System.out.println("[System-Engine] Local setup complete. Holding traffic and waiting for Cache-Service to finish...");

        // 3. Block the main thread execution until the cache warming thread is 100% complete
        cacheWarmupThread.join();

        System.out.println("[System-Engine] Dependency check passed! All services are green.");
        System.out.println("[System-Engine] Server is now listening on port 8080. Boot complete!");
    }
}