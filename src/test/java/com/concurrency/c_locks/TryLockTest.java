package com.concurrency.c_locks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TryLockTest {

    private AtomicInteger globalCounter;
    private Lock mockLock;

    @BeforeEach
    void setUp() {
        globalCounter = new AtomicInteger(0);
        mockLock = Mockito.mock(Lock.class);
    }

    @Test
    @DisplayName("Should increment local backlog when lock cannot be acquired")
    void testLockContentionIncrementsBacklog() throws InterruptedException {
        // Force tryLock to always return false (simulating another thread holding it)
        when(mockLock.tryLock()).thenReturn(false);

        // We set a small target quota so it breaks quickly
        SupportChatbot bot = new SupportChatbot("Test-Bot", mockLock, globalCounter, 5);

        bot.start();
        Thread.sleep(150); // Let it run a few loops
        bot.interrupt();   // Safely stop the loop
        bot.join();

        // Assert: The bot should have accumulated local backlog items because it could never sync
        assertTrue(bot.getLocalBacklogCount() > 0, "Backlog should accumulate when lock is unavailable");
        assertEquals(0, globalCounter.get(), "Global counter shouldn't change if lock is never acquired");
    }

    @Test
    @DisplayName("Should clear backlog and update global counter when lock is successfully acquired")
    void testSuccessfulLockSyncsBacklog() throws InterruptedException {
        // Force tryLock to return true so it can process work
        when(mockLock.tryLock()).thenReturn(true);

        // We want to hit the quota of 2 to stop the loop naturally
        SupportChatbot bot = new SupportChatbot("Test-Bot", mockLock, globalCounter, 2);

        bot.start();
        bot.join(2000); // Wait up to 2 seconds for it to finish naturally

        // Assert: It should successfully offload its data to the global counter
        assertTrue(globalCounter.get() >= 2, "Global counter should have met or exceeded the quota");
        assertEquals(0, bot.getLocalBacklogCount(), "Local backlog should have been cleared out");

        // Verify that unlock() was called at least once to prevent leaks
        verify(mockLock, atLeastOnce()).unlock();
    }

    @Test
    @DisplayName("Deterministic Integration Test: Multiple bots should hit exact quota using real lock")
    void testRealLockIntegrationWithMultipleBots() throws InterruptedException {
        Lock realLock = new ReentrantLock();
        AtomicInteger realCounter = new AtomicInteger(0);
        int targetQuota = 10;

        SupportChatbot botAlpha = new SupportChatbot("Alpha", realLock, realCounter, targetQuota);
        SupportChatbot botBeta = new SupportChatbot("Beta", realLock, realCounter, targetQuota);

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