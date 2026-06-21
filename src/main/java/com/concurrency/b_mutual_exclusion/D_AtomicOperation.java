package com.concurrency.b_mutual_exclusion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

class ApiAnalytics {
    private final AtomicLong totalRequestsServed = new AtomicLong(0);

    public void incrementRequestCount() {
        // This is now a thread-safe, atomic operation.
        // Multiple threads can call this simultaneously without losing data.
        totalRequestsServed.incrementAndGet();
    }

    public long getTotalRequestsServed() {
        return totalRequestsServed.get();
    }
}

public class D_AtomicOperation {
    public static void main(String[] args) { // No longer needs 'throws InterruptedException'
        ApiAnalytics analytics = new ApiAnalytics();
        int expectedTotalRequests = 100_000;

        // AutoCloseable handles shutdown and awaiting termination automatically at the '}'
        try (ExecutorService webServerThreadPool = Executors.newFixedThreadPool(8)) {
            for (int i = 0; i < expectedTotalRequests; i++) {
                webServerThreadPool.submit(analytics::incrementRequestCount);
            }
        } // <--- Thread pool shuts down and waits here automatically

        System.out.println("====== SERVER METRICS REPORT ======");
        System.out.println("Expected API Requests: " + expectedTotalRequests);
        System.out.println("Actual API Requests:   " + analytics.getTotalRequestsServed());
    }
}