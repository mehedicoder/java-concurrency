package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class BlackBoxProblemSolution {

    public void handleWebRequest(String userId, String orderId) throws Exception {
        System.out.format("[Thread: %s] Processing web request for User: %s\n",
                Thread.currentThread().getName(), userId);

        // Every request gets its own self-contained structural scope.
        // We set a defensive 2-second timeout so the request can never hang for 100 seconds!
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                cfg -> cfg.withTimeout(Duration.ofSeconds(2)).withName("OrderProcessing"))) {

            Subtask<String> inventoryTask = scope.fork(() -> checkInventoryStock(orderId));
            Subtask<String> paymentTask = scope.fork(() -> chargeCreditCard(userId));

            try {
                scope.join();
                String receipt = inventoryTask.get() + " | " + paymentTask.get();
                System.out.println("Receipt composed: " + receipt);
            } catch (StructuredTaskScope.TimeoutException e) {
                // Handle the timeout gracefully
                System.err.format("[WARN] Order processing timed out for user: %s. Rolling back transaction.\n", userId);
            }
        }
        // No more SHARED_POOL.shutdown() needed! The closing bracket auto-cleans everything.
    }

    private String checkInventoryStock(String orderId) throws InterruptedException {
        Thread.sleep(100_000); // This will now be safely cut off by the 2-second timeout
        return "STOCKED";
    }

    private String chargeCreditCard(String userId) throws InterruptedException {
        Thread.sleep(100_000);
        return "PAID";
    }

    public static void main(String[] args) throws Exception {
        BlackBoxProblemSolution controller = new BlackBoxProblemSolution();

        Thread t1 = new Thread(() -> { try { controller.handleWebRequest("Alice_99", "ORD-11"); } catch(Exception e){ e.printStackTrace(); } }, "exec-1");
        Thread t2 = new Thread(() -> { try { controller.handleWebRequest("Bob_44",   "ORD-22"); } catch(Exception e){ e.printStackTrace(); } }, "exec-2");

        t1.start();
        t2.start();
    }
}