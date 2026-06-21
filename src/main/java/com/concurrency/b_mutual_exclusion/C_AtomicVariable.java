package com.concurrency.b_mutual_exclusion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class PostMetrics {
    // A shared, thread-safe counter for a high-traffic social media post
    private final AtomicInteger likeCount = new AtomicInteger(0);

    // Simulated action when a global user clicks the "Like" button
    public void registerLike() {
        // This is an atomic operation. Read-modify-write happens in one seamless step.
        likeCount.incrementAndGet();
    }

    public int getLikeCount() {
        return likeCount.get();
    }
}

public class C_AtomicVariable {
    public static void main(String[] args) {
        PostMetrics viralPost = new PostMetrics();

        // Simulating 20,000,000 total interactions split across two main traffic regions
        int clicksPerRegion = 10_000_000;

        // Using Try-With-Resources (AutoCloseable ExecutorService) to manage our execution
        try (ExecutorService trafficSimulator = Executors.newFixedThreadPool(8)) {

            // Region 1: High-volume traffic coming from North American servers
            trafficSimulator.submit(() -> {
                for (int i = 0; i < clicksPerRegion; i++) {
                    viralPost.registerLike();
                }
            });

            // Region 2: Simultaneous high-volume traffic coming from European servers
            trafficSimulator.submit(() -> {
                for (int i = 0; i < clicksPerRegion; i++) {
                    viralPost.registerLike();
                }
            });

        } // The ExecutorService automatically shuts down and waits for both tasks to complete here.

        // --- Production Output ---
        System.out.println("====== SOCIAL MEDIA METRICS ======");
        System.out.println("Expected Likes: 20000000");
        System.out.println("Actual Likes:   " + viralPost.getLikeCount());

        if (viralPost.getLikeCount() == 20_000_000) {
            System.out.println("\nSUCCESS: AtomicInteger flawlessly handled 20 million concurrent requests!");
        }
    }
}
