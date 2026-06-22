package com.concurrency.j_jmm;

import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiRateLimiterFixTest {

    // JUnit will run this method 1000 separate times
    @RepeatedTest(1000)
    public void testConcurrentRequests() throws InterruptedException {
        B_ApiRateLimiterFix limiter = new B_ApiRateLimiterFix();
        ExecutorService service = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(1);

        service.submit(() -> {
            try {
                latch.await();
                limiter.allowRequest();
            } catch (Exception e) {}
        });

        service.submit(() -> {
            try {
                latch.await();
                limiter.allowRequest();
            } catch (Exception e) {}
        });

        latch.countDown();
        service.shutdown();
        service.awaitTermination(1, TimeUnit.SECONDS);

        // If this fails, JUnit records it but keeps going on the next repetition!
        assertEquals(2, limiter.getRequestCount());
    }
}