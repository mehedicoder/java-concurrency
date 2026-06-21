package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AtomicOperationTest {

    private ApiAnalytics analytics;

    @BeforeEach
    void setUp() {
        // Instantiate a fresh metrics wrapper before every test to guarantee clean state isolation
        analytics = new ApiAnalytics();
    }

    @Test
    @DisplayName("Should accurately increment counter sequentially under single-threaded control")
    void testSequentialIncrements() {
        // Act
        analytics.incrementRequestCount();
        analytics.incrementRequestCount();

        // Assert
        assertEquals(2, analytics.getTotalRequestsServed(),
                "Sequential increment failed to modify the underlying tracking register correctly.");
    }

    @Test
    @DisplayName("Should prevent any data loss or telemetry drops under heavy multi-threaded execution pools")
    void testConcurrentIncrementsPreserveMetricsIntegrity() throws InterruptedException {
        // Arrange
        int expectedTotalRequests = 100_000; // Matches your exact workload criteria
        int threadPoolSize = 8;             // Distribute across 8 worker threads

        // Act & Assert
        // Deploying a multi-threaded virtual executor pool using try-with-resources for clean thread scoping
        try (ExecutorService webServerThreadPool = Executors.newFixedThreadPool(threadPoolSize)) {
            for (int i = 0; i < expectedTotalRequests; i++) {
                webServerThreadPool.submit(analytics::incrementRequestCount);
            }

            // Initiate a controlled pool shutdown process
            webServerThreadPool.shutdown();

            // Enforce a strict termination timeout to protect local builds or CI/CD pipelines from freezing if an unexpected hang occurs
            boolean completedOnTime = webServerThreadPool.awaitTermination(5, TimeUnit.SECONDS);

            assertTrue(completedOnTime, "The parallel web server pool timed out before finishing processing tasks.");
        }

        // Verify that the non-blocking AtomicLong handled every concurrent execution without overlapping data drops
        assertEquals(expectedTotalRequests, analytics.getTotalRequestsServed(),
                String.format("Telemetry data loss detected! Expected tracking metrics register to hit exactly %d requests but reached %d",
                        expectedTotalRequests, analytics.getTotalRequestsServed()));
    }
}