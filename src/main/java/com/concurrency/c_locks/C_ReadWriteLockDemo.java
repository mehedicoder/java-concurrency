package com.concurrency.c_locks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class MarketTickerFeed {
    // Shared state containing volatile financial market pricing
    private final Map<String, Double> stockPrices = new HashMap<>();

    // ReadWriteLock controlling concurrent access routes
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public MarketTickerFeed() {
        // Initialize market items
        stockPrices.put("AAPL", 175.00);
        stockPrices.put("GOOG", 150.00);
        stockPrices.put("MSFT", 420.00);
    }

    // High frequency action: Unlimited threads can safely call this simultaneously
    public double getPrice(String ticker) {
        readLock.lock();
        try {
            return stockPrices.getOrDefault(ticker, 0.0);
        } finally {
            readLock.unlock();
        }
    }

    // Low frequency action: Mutex behavior, blocks all readers while executing
    public void updatePrice(String ticker, double newPrice) {
        writeLock.lock();
        try {
            stockPrices.put(ticker, newPrice);
            System.out.println("[MARKET WRITER] Updated " + ticker + " price to $" + newPrice);
        } finally {
            writeLock.unlock();
        }
    }

    // Diagnostic method to safely monitor reader metrics
    public int getActiveReaderCount() {
        return rwLock.getReadLockCount();
    }
}

public class C_ReadWriteLockDemo {
    public static void main(String[] args) {
        MarketTickerFeed tickerFeed = new MarketTickerFeed();

        // 10 Reader Threads simulating user traffic and 2 Writer Threads simulating price updates
        try (ExecutorService marketCluster = Executors.newFixedThreadPool(12)) {

            // Spawn 10 Real-time Reader Tasks
            for (int i = 0; i < 10; i++) {
                final int readerId = i;
                marketCluster.submit(() -> {
                    for (int j = 0; j < 5; j++) {
                        double price = tickerFeed.getPrice("AAPL");
                        System.out.println("Reader-" + readerId + " read AAPL: $" + price
                                + " | Active Readers: " + tickerFeed.getActiveReaderCount());
                        try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                });
            }

            // Spawn 2 Price Injector Writer Tasks
            for (int i = 0; i < 2; i++) {
                final int writerId = i;
                marketCluster.submit(() -> {
                    double mockPriceMultiplier = 175.50 + writerId;
                    for (int j = 0; j < 2; j++) {
                        tickerFeed.updatePrice("AAPL", mockPriceMultiplier + j);
                        try { Thread.sleep(120); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                });
            }
        } // Automatically joins and safely destroys threads here
    }
}