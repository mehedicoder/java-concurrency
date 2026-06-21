package com.concurrency.d_liveness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

class AbandonedLockTest {

    private Field queueCapacityField;

    @BeforeEach
    void setUp() throws NoSuchFieldException {
        // Cache and open access to the private static internal counter to ensure clean state isolation
        queueCapacityField = TransactionProcessor.class.getDeclaredField("globalTransferQueueCapacity");
        queueCapacityField.setAccessible(true);
    }

    @Test
    @DisplayName("Should prove that an unhandled runtime exception causes a primary lock to be permanently abandoned")
    void testExceptionCausesAbandonedLockVulnerability() throws Exception {
        // Arrange: Separate fresh lock references for isolation
        ReentrantLock checkingAccountMutex = new ReentrantLock();
        ReentrantLock savingsAccountMutex = new ReentrantLock();

        TransactionProcessor vulnerableWorker = new TransactionProcessor(
                "Test-Tx-Cluster",
                checkingAccountMutex,
                savingsAccountMutex
        );

        // Fast-forward to index 491 to trigger the crash exception immediately
        queueCapacityField.set(null, 495);

        // Act
        vulnerableWorker.start();
        vulnerableWorker.join(1500); // Allow thread processing to encounter the crash

        // Assert
        assertFalse(vulnerableWorker.isAlive(), "Worker thread should have terminated after the crash.");

        // Success criteria: Prove the lock was abandoned
        assertTrue(checkingAccountMutex.isLocked(),
                "CRITICAL VULNERABILITY: The primary lock was left abandoned and remains locked after thread death!");

        // Prove the secondary lock was released via its finally block
        assertFalse(savingsAccountMutex.isLocked(),
                "The secondary lock should have been safely released via its corresponding finally block.");

        // Clean Up Phase: We explicitly DO NOT call checkingAccountMutex.unlock() here
        // to avoid IllegalMonitorStateException. By allowing checkingAccountMutex to drop out of
        // this method scope, the GC collects the stale object cleanly, preventing test pollution.
        System.out.println("[Test] Isolation verified successfully. Discarding locked assets.");
    }

    @Test
    @DisplayName("Should process transactions smoothly when remaining within nominal non-crashing data ranges")
    void testStandardExecutionWithoutExceptions() throws Exception {
        // Arrange
        ReentrantLock checkingAccountMutex = new ReentrantLock();
        ReentrantLock savingsAccountMutex = new ReentrantLock();

        TransactionProcessor standardWorker = new TransactionProcessor(
                "Test-Tx-Healthy",
                checkingAccountMutex,
                savingsAccountMutex
        );

        // Compress the initial workload scale down to 5 to keep build pipeline executions hyper-fast
        queueCapacityField.set(null, 5);

        // Act
        standardWorker.start();
        standardWorker.join(1000);

        // Assert
        assertFalse(standardWorker.isAlive(), "Worker thread should have finished its small loop and exited cleanly.");
        assertFalse(checkingAccountMutex.isLocked(), "Primary account lock should not remain locked after healthy termination.");
        assertFalse(savingsAccountMutex.isLocked(), "Secondary account lock should not remain locked after healthy termination.");

        int finalCapacityValue = queueCapacityField.getInt(null);
        assertEquals(0, finalCapacityValue, "The transfer engine should have completely exhausted the unallocated queue.");
    }
}