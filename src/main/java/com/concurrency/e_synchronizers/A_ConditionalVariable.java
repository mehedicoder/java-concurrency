package com.concurrency.e_synchronizers;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class SettlementProcessor extends Thread {
    private final int shardId;

    // Explicit mutual exclusion components shared across our cluster nodes
    private static final Lock pipelineLock = new ReentrantLock();
    private static final Condition sequenceUpdated = pipelineLock.newCondition();

    // Shared global transactional registry keeping track of remaining events to clear down
    private static int outstandingTransactions = 11;
    private static final int TOTAL_SHARDS = 5;

    public SettlementProcessor(int shardId) {
        super("Settlement-Shard-" + shardId);
        this.shardId = shardId;
    }

    @Override
    public void run() {
        while (outstandingTransactions > 0) {
            pipelineLock.lock();
            try {
                // GUARD: Check if the current trade index sequence does NOT match this Shard's allocation rule
                while ((shardId != outstandingTransactions % TOTAL_SHARDS) && outstandingTransactions > 0) {
                    System.out.format("[%s] Sequence out of order for this shard. Yielding and waiting...\n", this.getName());

                    // Atomically release the lock and block until another shard broadcasts a progression signal
                    sequenceUpdated.await();
                }

                // Double-check the global state loop escape boundary inside the critical section
                if (outstandingTransactions > 0) {
                    outstandingTransactions--; // Securely clear the transaction order

                    System.out.format("[%s] Successfully cleared financial trade ledger settlement. Remaining queue: %d\n",
                            this.getName(), outstandingTransactions);

                    // Broadcast a signal to awaken all waiting shard threads to re-evaluate their sequence turn
                    sequenceUpdated.signalAll();
                }
            } catch (InterruptedException e) {
                System.err.format("[%s] Pipeline worker thread interrupted.\n", this.getName());
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.format("[%s] Critical ledger corruption encountered: %s\n", this.getName(), e.getMessage());
            } finally {
                pipelineLock.unlock();
            }
        }
    }
}

public class A_ConditionalVariable {
    public static void main(String[] args) {
        System.out.println("[Clearing-Engine] Activating deterministic transaction sequencing cluster...");

        // Deploy 5 regional cluster shard nodes to process the financial data pool sequentially
        for (int i = 0; i < 5; i++) {
            new SettlementProcessor(i).start();
        }
    }
}