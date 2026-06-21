package com.concurrency.g_structured;

import java.util.concurrent.*;

public class BlackBoxProblem {

    public void handleWebRequest(String userId, String orderId) throws Exception {
        System.out.format("[Thread: %s] Processing web request for User: %s\n",
                Thread.currentThread().getName(), userId);

        try (ExecutorService pool = Executors.newFixedThreadPool(10)) {
            // Dispatched tasks lose their relationship to the parent controller thread
            Future<String> inventoryTask = pool.submit(() -> checkInventoryStock(orderId));
            Future<String> paymentTask = pool.submit(() -> chargeCreditCard(userId));

            // Block and gather
            String receipt = inventoryTask.get() + " | " + paymentTask.get();
        }
    }

    private String checkInventoryStock(String orderId) throws InterruptedException {
        // Imagine this thread hangs indefinitely here due to a missing database timeout!
        Thread.sleep(1000);
        return "STOCKED";
    }

    private String chargeCreditCard(String userId) throws InterruptedException {
        Thread.sleep(5000);
        return "PAID";
    }

    public static void main(String[] args) throws Exception {
        BlackBoxProblem controller = new BlackBoxProblem();

        // Simulating three separate concurrent web server request threads hitting our app
        new Thread(() -> { try { controller.handleWebRequest("Alice_99", "ORD-11"); } catch(Exception e){} }, "http-nio-8080-exec-1").start();
        new Thread(() -> { try { controller.handleWebRequest("Bob_44",   "ORD-22"); } catch(Exception e){} }, "http-nio-8080-exec-2").start();
    }
}