package com.concurrency.c_locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

class TryLockTest {

    private AtomicInteger globalCounter;
    private ReentrantLock realLock; // Changed type to ReentrantLock to access state methods

    @BeforeEach
    void setUp() {
        globalCounter = new AtomicInteger(0);
        this.realLock = new ReentrantLock();
    }

    @Test
    @DisplayName("Should increment local backlog when lock cannot be acquired")
    void testLockContentionIncrementsBacklog() throws InterruptedException {
        // FIX: Instead of Mockito when(), manually acquire the lock on the main thread.
        // This guarantees that any call to bot's tryLock() will instantly return false.
        realLock.lock();

        try {
            // We set a small target quota so it breaks quickly
            SupportChatbot bot = new SupportChatbot("Test-Bot", realLock, globalCounter, 5);

            bot.start();
            Thread.sleep(150); // Let it run a few loops accumulating backlog
            bot.interrupt();   // Safely stop the loop
            bot.join(1000);

            // Assert: The bot should have accumulated local backlog items because it could never sync
            assertTrue(bot.getLocalBacklogCount() > 0, "Backlog should accumulate when lock is unavailable");
            assertEquals(0, globalCounter.get(), "Global counter shouldn't change if lock is never acquired");
        } finally {
            // Always clean up the lock to avoid side effects across your suite tests
            realLock.unlock();
        }
    }

    @Test
    @DisplayName("Should clear backlog and update global counter when lock is successfully acquired")
    void testSuccessfulLockSyncsBacklog() throws InterruptedException {
        // FIX: The lock is completely open by default, so tryLock() will naturally return true.
        // No Mockito stubbing needed.

        // We want to hit the quota of 2 to stop the loop naturally
        SupportChatbot bot = new SupportChatbot("Test-Bot", realLock, globalCounter, 2);

        bot.start();
        bot.join(2000); // Wait up to 2 seconds for it to finish naturally

        // Assert: It should successfully offload its data to the global counter
        assertTrue(globalCounter.get() >= 2, "Global counter should have met or exceeded the quota");
        assertEquals(0, bot.getLocalBacklogCount(), "Local backlog should have been cleared out");

        // FIX: Instead of Mockito verify(), use the ReentrantLock's built-in API
        // to ensure it was properly unlocked and didn't leak its held state.
        assertFalse(realLock.isLocked(), "The lock was leaked and not released via unlock()!");
    }

    @Test
    @DisplayName("Deterministic Integration Test: Multiple bots should hit exact quota using real lock")
    void testRealLockIntegrationWithMultipleBots() throws InterruptedException {
        ReentrantLock testIntegrationLock = new ReentrantLock();
        AtomicInteger realCounter = new AtomicInteger(0);
        int targetQuota = 10;

        SupportChatbot botAlpha = new SupportChatbot("Alpha", testIntegrationLock, realCounter, targetQuota);
        SupportChatbot botBeta = new SupportChatbot("Beta", testIntegrationLock, realCounter, targetQuota);

        botAlpha.start();
        botBeta.start();

        botAlpha.join(3000);
        botBeta.join(3000);

        // Ensure both threads shut down safely and reached the target threshold
        assertFalse(botAlpha.isAlive(), "Bot Alpha failed to terminate");
        assertFalse(botBeta.isAlive(), "Bot Beta failed to terminate");
        assertTrue(realCounter.get() >= targetQuota, "Bots failed to complete the total work quota");
    }
}