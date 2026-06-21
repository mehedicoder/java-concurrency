package com.concurrency.f_async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class C_AdvancedCompletableFuture {

    public static void main(String[] args) {
        System.out.format("[%s] Initializing Logistics Distribution Control...\n", Thread.currentThread().getName());

        // Dedicated processing pool
        ExecutorService logisticsPool = Executors.newFixedThreadPool(4);

        try {
            // 1. Kick off an independent fast validation task
            CompletableFuture<Boolean> weightValidationFuture = CompletableFuture.supplyAsync(() -> {
                System.out.format("[%s] [Step 1] Scanning cargo mass and distribution indices...\n", Thread.currentThread().getName());
                sleep(1500); // 1.5 seconds
                System.out.format("[%s] [Step 1 Completed] Weight metrics within safe thresholds.\n", Thread.currentThread().getName());
                return true;
            }, logisticsPool);

            // 2. Build a dependent pipeline using .thenCompose()
            // This represents a long-running workflow where Step 3 strictly waits for Step 2.
            CompletableFuture<String> clearancePipeline = CompletableFuture.supplyAsync(() -> {
                        System.out.format("[%s] [Step 2] Transmitting cross-border customs paperwork...\n", Thread.currentThread().getName());

                        // --- SIMULATING THE LONG RUNNING 5-MINUTE DELAY ---
                        // For demonstration, we will simulate an extended delay (e.g., 6 seconds)
                        // but protect it with a strict SLA timeout window below.
                        sleep(6000);

                        System.out.format("[%s] [Step 2 Completed] International customs clearance approved.\n", Thread.currentThread().getName());
                        return "AUTH-99281-XM";
                    }, logisticsPool)

                    // PROTECT THE ENGINE: If customs takes longer than 4 seconds, abort the pipeline!
                    .orTimeout(4, TimeUnit.SECONDS)

                    // HANDLE EXCEPTIONS FLUIDLY: If it timeouts or fails, fall back to a backup route safely
                    .exceptionally(ex -> {
                        System.out.format("[%s] System Alert: Customs processing breached SLA or failed: %s\n",
                                Thread.currentThread().getName(), ex.getMessage());
                        System.out.format("[%s] Rerouting cargo shipment to domestic holding facilities...\n", Thread.currentThread().getName());
                        return "DOMESTIC-RE-ROUTE"; // Graceful fallback value
                    })

                    // DEPENDENT STEP 3: .thenCompose flatmaps the output of Step 2 directly into Step 3
                    .thenComposeAsync(clearanceToken -> CompletableFuture.supplyAsync(() -> {
                        System.out.format("[%s] [Step 3] Token [%s] verified. Generating flight departure manifest...\n",
                                Thread.currentThread().getName(), clearanceToken);
                        sleep(1000);
                        return "MANIFEST-FINALIZED-DELIVERY-SUCCESS";
                    }, logisticsPool), logisticsPool);

            // 3. Coordinate the final aggregation
            // We print updates as the fast validation finishes early, while the main thread waits for the final outcome.
            weightValidationFuture.thenAccept(valid -> {
                if (valid) System.out.println("[Log] Ground crew preparing plane loading pads...");
            });

            System.out.println("\n[Main Thread] Ground operations are idling. Awaiting dependent clearance pipelines...\n");

            // Block the main execution track safely until the entire pipeline resolves
            String finalLogisticsStatus = clearancePipeline.join();
            System.out.format("\n[%s] Global Processing Complete. System Result Token: %s\n",
                    Thread.currentThread().getName(), finalLogisticsStatus);

        } finally {
            logisticsPool.shutdown();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}