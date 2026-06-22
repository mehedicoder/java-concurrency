package com.concurrency.j_jmm;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiRateLimiterStandardTest {

    @Test
    public void verifyFlakyBehaviorOfStandardTest() throws InterruptedException {
        int totalIterations = 1000;
        int countIsTwo = 0;
        int countIsOne = 0;

        // Use a single cached thread pool to avoid destroying/creating 2000 threads sequentially
        ExecutorService service = Executors.newFixedThreadPool(2);

        try {
            for (int i = 0; i < totalIterations; i++) {
                // IMPORTANT: Use the class with the bug here (A_ApiRateLimiterFix or A_ApiRateLimiter)
                A_ApiRateLimiter limiter = new A_ApiRateLimiter(2);
                CountDownLatch latch = new CountDownLatch(1);

                Future<?> f1 = service.submit(() -> {
                    try {
                        latch.await();
                        limiter.allowRequest();
                    } catch (Exception ignored) {}
                });

                Future<?> f2 = service.submit(() -> {
                    try {
                        latch.await();
                        limiter.allowRequest();
                    } catch (Exception ignored) {}
                });

                // Fire both threads simultaneously
                latch.countDown();

                // Wait for both tasks to explicitly finish this iteration
                f1.get();
                f2.get();

                int finalCount = limiter.getRequestCount();
                if (finalCount == 2) {
                    countIsTwo++;
                } else if (finalCount == 1) {
                    countIsOne++;
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            service.shutdown();
            service.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Print the distribution so you can see it in your build logs
        System.out.printf("Test finished. Result distribution -> Count=2: %d times, Count=1: %d times.%n",
                countIsTwo, countIsOne);

        // BULLETPROOF ASSERTIONS:
        // Ensure no weird states occurred (like 0 or 3)
        assertEquals(totalIterations, countIsTwo + countIsOne,
                "The total number of tracked outcomes does not match the iterations.");

        // Safely warn if the test hid the bug, without crashing the build:
        if (countIsOne == 0) {
            System.err.println("[WARNING] The standard test completely hid the bug this run! countIsOne was 0.");
        }
    }
}