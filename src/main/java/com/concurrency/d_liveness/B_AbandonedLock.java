package com.concurrency.d_liveness;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TransactionProcessor extends Thread {

    private final Lock primaryAccountLock;
    private final Lock secondaryAccountLock;

    // Shared transactional metric representing a global batch allocation pool
    private static int globalTransferQueueCapacity = 500;

    public TransactionProcessor(String workerName, Lock primaryAccountLock, Lock secondaryAccountLock) {
        super(workerName);
        this.primaryAccountLock = primaryAccountLock;
        this.secondaryAccountLock = secondaryAccountLock;
    }

    @Override
    public void run() {
        while (globalTransferQueueCapacity > 0) {

            // Acquire locks sequentially to initiate the ledger sync
            primaryAccountLock.lock();

            // CRITICAL ARCHITECTURAL FLAW (The Abandoned Lock Trap):
            // We lack a 'try' block immediately following the first lock.
            // If any runtime exception occurs here, the primary lock is abandoned forever!

            secondaryAccountLock.lock();
            try {
                // Simulate an unexpected data format or validation exception mid-flight
                if (globalTransferQueueCapacity == 490) {
                    throw new RuntimeException("CRITICAL: Ledger metadata sync corrupted!");
                }

                if (globalTransferQueueCapacity > 0) {
                    globalTransferQueueCapacity--;
                    System.out.format("[%s] Successfully verified balance swap. Remaining transfers: %d\n",
                            this.getName(), globalTransferQueueCapacity);
                }
            } finally {
                // Standard unlocking block
                secondaryAccountLock.unlock();
            }

            // If an exception is thrown above, this line is bypassed completely!
            primaryAccountLock.unlock();

            // Brief pacing pause simulating network routing overhead
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

public class B_AbandonedLock {
    public static void main(String[] args) {
        System.out.println("[System] Initializing Ledger Transfer Sync Clusters...");

        // Three shared accounts represented as locks requiring absolute mutual exclusion
        Lock checkingAccountMutex = new ReentrantLock();
        Lock savingsAccountMutex = new ReentrantLock();
        Lock investmentAccountMutex = new ReentrantLock();

        // Worker Alpha manages Checking -> Savings transfers
        Thread workerAlpha = new TransactionProcessor("Tx-Cluster-Alpha",
                checkingAccountMutex, savingsAccountMutex);

        // Worker Beta manages Savings -> Investment transfers
        Thread workerBeta = new TransactionProcessor("Tx-Cluster-Beta",
                savingsAccountMutex, investmentAccountMutex);

        // Worker Gamma manages Investment -> Checking transfers (Circular path)
        Thread workerGamma = new TransactionProcessor("Tx-Cluster-Gamma",
                investmentAccountMutex, checkingAccountMutex);

        // Start processing. It will either deadlock due to circular resource acquisition,
        // OR it will throw the runtime exception at index 490 and permanently abandon the lock!
        workerAlpha.start();
        workerBeta.start();
        workerGamma.start();
    }
}
