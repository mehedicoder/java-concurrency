package com.concurrency.f_async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DetachedBackgroundJobAsync {

    private static final ExecutorService backgroundJobPool = Executors.newFixedThreadPool(2);

    public static void main(String[] args) throws Exception {
        System.out.println("[HTTP Thread] POST /api/reports/trigger-export received.");

        // Trigger a detached task. It escapes the local lifecycle and continues running
        // long after this method finishes execution.
        triggerLongRunningPdfExport("ANNUAL_TAX_REPORT_2026");

        System.out.println("[HTTP Thread] Instantly returning Response: HTTP 202 (Accepted). Transaction complete.");

        // Simulating the application continuing to live while the detached job processes
        Thread.sleep(4000);
        backgroundJobPool.shutdown();
    }

    public static void triggerLongRunningPdfExport(String reportName) {
        CompletableFuture.runAsync(() -> {
            System.out.println("\n[Asynchronous Worker Thread] Beginning heavy compilation matrix for: " + reportName);
            try {
                Thread.sleep(2500); // Simulate processing heavy document generation
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("[Asynchronous Worker Thread] Report finalized and uploaded to secure cloud storage bucket.");
        }, backgroundJobPool);
    }
}