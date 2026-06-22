package com.concurrency.j_jmm;

import java.util.concurrent.atomic.AtomicInteger;

// The Production Class containing a silent JMM bug creates intentional data race
public class A_ApiRateLimiterFix {
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private int limit = 100;

    public A_ApiRateLimiterFix(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        this.limit = limit;
    }
    // Checks if the request is within bounds and increments the counter
    public boolean allowRequest() {
        while (true) {
            int current = requestCount.get(); // 1. Read the current value

            if (current >= limit) {
                return false; // 2. Check: If we hit the limit, reject immediately
            }

            // 3. Act atomically:
            // Attempt to change 'current' to 'current + 1'.
            // If another thread modified 'requestCount' in the split-second since we read it,
            // compareAndSet will FAIL (return false). This protects us from the race!
            if (requestCount.compareAndSet(current, current + 1)) {
                return true; // Successfully incremented without anyone interfering
            }

            // If compareAndSet failed, the loop automatically retries with the fresh value.
        }
    }

    public int getRequestCount() {
        return requestCount.get();
    }
}