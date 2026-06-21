package com.concurrency.a_thread_process;

/**
 * Simulates a heavy, asynchronous background task: Generating a PDF invoice.
 */
class InvoiceGenerator implements Runnable {
    @Override
    public void run() {
        System.out.println("[Invoice-Thread] Starting PDF generation...");
        try {
            // Simulate heavy cryptographic signing, template rendering, and DB fetching
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            System.err.println("[Invoice-Thread] Interrupted while generating PDF!");
            Thread.currentThread().interrupt();
            return;
        }
        System.out.println("[Invoice-Thread] PDF successfully generated and uploaded to S3 bucket.");
    }
}

public class C_ThreadLifecycle {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Main-Checkout-Thread] Customer clicked 'Buy Now'. Initiating checkout flow...");

        // 1. Create the worker thread (NEW State)
        Thread invoiceWorker = new Thread(new InvoiceGenerator(), "Invoice-Worker-1");
        System.out.format("   -> Invoice Worker State: %s (Thread is created but not started)\n", invoiceWorker.getState());

        System.out.println("\n[Main-Checkout-Thread] Spawning background worker to handle PDF generation...");
        // 2. Start the thread (RUNNABLE / TIMED_WAITING State)
        invoiceWorker.start();
        System.out.format("   -> Invoice Worker State: %s (Thread is now running or ready to run)\n", invoiceWorker.getState());

        System.out.println("\n[Main-Checkout-Thread] Main thread continues processing: reserving inventory & charging credit card...");
        // Simulate local main thread operations taking a brief moment
        Thread.sleep(500);
        System.out.format("   -> Invoice Worker State: %s (Worker is currently inside Thread.sleep() simulating heavy work)\n", invoiceWorker.getState());

        System.out.println("\n[Main-Checkout-Thread] Financial transaction complete. Blocking to await final PDF attachment...");

        // 3. Block the main thread until the worker finishes (WAITING -> TERMINATED State)
        invoiceWorker.join();

        System.out.format("   -> Invoice Worker State: %s (Worker has finished execution safely)\n", invoiceWorker.getState());

        System.out.println("\n[Main-Checkout-Thread] Checkout Complete! Showing confirmation screen with the generated PDF link to customer.");
    }
}