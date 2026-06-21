package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AtomicVariableTest {

    @Test
    @DisplayName("Should accurately increment like counts across multiple concurrent traffic pools without data loss")
    void testConcurrentLikesAccumulation() throws InterruptedException {
        PostMetrics viralPost = new PostMetrics();
        int clicksPerRegion = 50_000; // Scaled for optimal test execution speeds
        int totalExpectedLikes = clicksPerRegion * 2;

        // Use try-with-resources to enforce thread synchronization and automated cleanup
        try (ExecutorService trafficSimulator = Executors.newFixedThreadPool(4)) {

            // Simulating parallel traffic pool 1 (e.g., North America server)
            trafficSimulator.submit(() -> {
                for (int i = 0; i < clicksPerRegion; i++) {
                    viralPost.registerLike();
                }
            });

            // Simulating parallel traffic pool 2 (e.g., Europe server)
            trafficSimulator.submit(() -> {
                for (int i = 0; i < clicksPerRegion; i++) {
                    viralPost.registerLike();
                }
            });

            // Initiate a controlled shutdown of our execution pool
            trafficSimulator.shutdown();
            boolean finishedCleanly = trafficSimulator.awaitTermination(5, TimeUnit.SECONDS);

            // Verify that the workers did not crash or hang indefinitely
            assertTrue(finishedCleanly, "Traffic simulator threads timed out or deadlock occurred.");
        }

        // Assert that the AtomicInteger flawlessly handled every parallel increment sequence
        assertEquals(totalExpectedLikes, viralPost.getLikeCount(),
                String.format("Data loss detected! Expected %d likes but actual count was %d",
                        totalExpectedLikes, viralPost.getLikeCount()));
    }
}