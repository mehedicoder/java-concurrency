package com.concurrency.j_jmm;

// The Production Class containing a silent JMM bug creates intentional data race
public class A_ApiRateLimiter {
    private int requestCount = 0;
    private int limit = 100;

    public A_ApiRateLimiter(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        this.limit = limit;
    }
    // Checks if the request is within bounds and increments the counter
    public boolean allowRequest() {
        if (requestCount < limit) {
            // BUG: Non-atomic increment on a non-volatile variable!
            requestCount++;
            return true;
        }
        return false;
    }

    public int getRequestCount() {
        return requestCount;
    }
}