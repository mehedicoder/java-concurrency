package com.concurrency.c_locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class SupportChatbot extends Thread {

    private final AtomicInteger globalVipTicketsResolved;
    private final Lock vipRegistryLock;
    private int localBacklogCount = 0;
    private final int targetQuota;

    // Dependency Injection makes this class incredibly easy to unit test
    public SupportChatbot(String botName, Lock vipRegistryLock, AtomicInteger globalCounter, int targetQuota) {
        super(botName);
        this.vipRegistryLock = vipRegistryLock;
        this.globalVipTicketsResolved = globalCounter;
        this.targetQuota = targetQuota;
    }

    // Getter for testing state assertions
    public int getLocalBacklogCount() {
        return this.localBacklogCount;
    }

    @Override
    public void run() {
        while (globalVipTicketsResolved.get() <= targetQuota) {
            if ((localBacklogCount > 0) && vipRegistryLock.tryLock()) {
                try {
                    // Update atomically inside the lock
                    globalVipTicketsResolved.addAndGet(localBacklogCount);
                    localBacklogCount = 0;
                    TimeUnit.MILLISECONDS.sleep(50); // Kept low for fast test execution
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } finally {
                    vipRegistryLock.unlock();
                }
            } else {
                try {
                    TimeUnit.MILLISECONDS.sleep(20);
                    localBacklogCount++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}

public class B_TryLockDemo {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[System] Booting multi-threaded AI Support Cluster...");

        // 1. Initialize the shared resources that the bots will compete for
        Lock sharedVipLock = new ReentrantLock();
        AtomicInteger sharedVipCounter = new AtomicInteger(0);
        int targetQuota = 20;

        // 2. Inject the dependencies into each chatbot instance
        Thread botAlpha = new SupportChatbot("Bot-Alpha", sharedVipLock, sharedVipCounter, targetQuota);
        Thread botBeta = new SupportChatbot("Bot-Beta", sharedVipLock, sharedVipCounter, targetQuota);

        long startTime = System.currentTimeMillis();

        // 3. Start processing threads concurrently
        botAlpha.start();
        botBeta.start();

        // 4. Await execution completion
        botAlpha.join();
        botBeta.join();

        long endTime = System.currentTimeMillis();

        System.out.println("--------------------------------------------------");
        System.out.println("[System] VIP Ticket handling quota met. Core shutdown complete.");
        System.out.format("Total execution time: %.2f seconds.\n", (endTime - startTime) / 1000.0);
        System.out.println("[System] Total VIP Tickets confirmed in Registry: " + sharedVipCounter.get());
    }
}