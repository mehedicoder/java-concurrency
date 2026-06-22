package com.concurrency.d_liveness;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class OrderBookStreamer extends Thread {

    private final Lock orderBookLock;
    private int successfulAllocations = 0;

    // Shared global liquidity pool representing remaining transactions available for execution
    private static int unallocatedTradePool = 500_000;

    public OrderBookStreamer(String pipelineName, Lock orderBookLock) {
        super(pipelineName);
        this.orderBookLock = orderBookLock;
    }

    @Override
    public void run() {
        while (unallocatedTradePool > 0) {
            // High-frequency attempt to acquire execution rights
            orderBookLock.lock();
            try {
                if (unallocatedTradePool > 0) {
                    unallocatedTradePool--;
                    successfulAllocations++;

                    // Controlled logging to highlight thread distribution metrics
                    if (unallocatedTradePool % 50_000 == 0) {
                        System.out.format("[%s] Processing trade index batch... Liquidity pool remaining: %d\n",
                                this.getName(), unallocatedTradePool);
                    }
                }
            } catch (Exception e) {
                System.err.format("[%s] Data validation failure occurred: %s\n", this.getName(), e.getMessage());
            } finally {
                orderBookLock.unlock();
            }
        }

        // Output production balance statistics to verify if any cluster suffered severe starvation
        System.out.format(">>> [Stream-Shutdown] %s successfully finalized %d transactional orders.\n",
                this.getName(), successfulAllocations);
    }
}

public class C_Starvation {
    public static void main(String[] args) {
        System.out.println("[Market Engine] Initializing high-frequency order-book streaming network...");

        // A single shared localized lock representing intense resource contention
        Lock orderBookLock = new ReentrantLock();

        // Spin up regional trading pipeline nodes competing for the exact same resource block
        // Without an explicit 'fair' scheduling algorithm or thread pools, certain threads
        // will aggressively hog CPU cache lines, starving others.
        for (int i = 0; i < 10; i++) {
            new OrderBookStreamer("HFT-Cluster-US-East-" + i, orderBookLock).start();
            new OrderBookStreamer("HFT-Cluster-EU-West-" + i, orderBookLock).start();
            new OrderBookStreamer("HFT-Cluster-APAC-South-" + i, orderBookLock).start();
        }
    }
}