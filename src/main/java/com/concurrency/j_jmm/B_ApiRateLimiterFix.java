package com.concurrency.j_jmm;

import java.util.concurrent.atomic.AtomicInteger;

// The Production Class containing a silent JMM bug creates intentional data race
public class B_ApiRateLimiterFix {
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final int limit = 100;

    // Checks if the request is within bounds and increments the counter
    public boolean allowRequest() {
        int currentRequestCount = requestCount.get();
        if (currentRequestCount < limit) {
            // BUG: Non-atomic increment on a non-volatile variable!
            requestCount.getAndIncrement();
            return true;
        }
        return false;
    }

    public int getRequestCount() {
        return requestCount.get();
    }
}