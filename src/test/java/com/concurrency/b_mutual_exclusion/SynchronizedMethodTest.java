package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SynchronizedMethodTest {

    @BeforeEach
    void setUp() throws Exception {
        // RESET STATIC STATE: Because 'balance' is a static field, it would normally carry over
        // from test to test. Reflection clears it to exactly 0.0 before each test executes.
        Field field = BankAccount.class.getDeclaredField("balance");
        field.setAccessible(true);
        field.set(null, 0.0);
    }

    @Test
    @DisplayName("Happy Path: Multiple heavy transaction processors should deposit concurrently without losing a single cent")
    void testConcurrentDeposits() {
        // Arrange
        int totalThreads = 4;
        int transactionsPerThread = 250_000; // 1,000,000 total transactions
        double depositAmount = 1.50;
        double expectedBalance = totalThreads * transactionsPerThread * depositAmount;

        // Act
        try (ExecutorService threadPool = Executors.newFixedThreadPool(totalThreads)) {
            for (int i = 0; i < totalThreads; i++) {
                threadPool.submit(() -> {
                    for (int j = 0; j < transactionsPerThread; j++) {
                        BankAccount.deposit(depositAmount);
                    }
                });
            }
        } // ExecutorService auto-closes and blocks here until all deposits finish cleanly

        // Assert
        assertEquals(expectedBalance, BankAccount.getBalance(),
                "The static synchronized method must completely eliminate financial data loss under multi-threaded load.");
    }

    @Test
    @DisplayName("Edge Case: Initial account balance should be exactly zero")
    void testInitialBalanceIsZero() {
        // Act & Assert
        assertEquals(0.0, BankAccount.getBalance(),
                "A fresh system check must always find a clean $0.0 starting balance.");
    }

    @Test
    @DisplayName("Edge Case: System should gracefully process fractional/floating-point values accurately")
    void testFractionalDeposits() {
        // Arrange
        double tinyAmount = 0.01; // Testing pennies
        int executionCycles = 10_000;
        double expectedSum = tinyAmount * executionCycles;

        // Act
        for (int i = 0; i < executionCycles; i++) {
            BankAccount.deposit(tinyAmount);
        }

        // Assert
        // Note: Floating-point math can introduce minor rounding errors in computers.
        // We use a tiny delta (0.0001) to ensure precision matching.
        assertEquals(expectedSum, BankAccount.getBalance(), 0.0001,
                "The application must maintain floating-point accumulation accuracy.");
    }

    @Test
    @DisplayName("Advanced/Stress Case: Forcing intense race condition window using a CountDownLatch starter pistol")
    void testExtremeLockContention() throws InterruptedException {
        // Arrange
        int simultaneousThreads = 2;
        int transactionsPerThread = 50_000;
        double depositAmount = 2.0;
        double expectedFinalBalance = simultaneousThreads * transactionsPerThread * depositAmount;

        CountDownLatch startingPistol = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(simultaneousThreads);

        // Act
        for (int i = 0; i < simultaneousThreads; i++) {
            executor.submit(() -> {
                try {
                    startingPistol.await(); // Trap all threads at the starting line
                    for (int j = 0; j < transactionsPerThread; j++) {
                        BankAccount.deposit(depositAmount);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startingPistol.countDown(); // FIRE! Both threads storm BankAccount.deposit() at the same instant
        executor.shutdown();
        boolean finishedCleanly = executor.awaitTermination(5, TimeUnit.SECONDS);

        // Assert
        assertTrue(finishedCleanly, "Threads locked up or deadlocked during execution.");
        assertEquals(expectedFinalBalance, BankAccount.getBalance(),
                "Even under identical-millisecond stress, the synchronized method must force a neat queue.");
    }
}