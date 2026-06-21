package com.concurrency.f_async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonBlockingGatewayAsync {

    private static final ExecutorService proxyWorkerPool = Executors.newFixedThreadPool(4);

    public static void main(String[] args) {
        System.out.println("[Gateway Routing Core] Main engine initialized.");

        // Dispatch an entire pipeline. Control returns to the calling code IN A FRACTION OF A MILLISECOND.
        // Neither the main thread nor any proxy orchestration threads stall or wait.
        processIncomingHttpRequest("/api/v1/user/profile")
                .thenAccept(response -> System.out.println("[Gateway Outbound Routing] HTTP 200 Stream Dispatched to Client: " + response));

        System.out.println("[Gateway Routing Core] Immediate Return! I can handle another 50,000 requests right now.");

        proxyWorkerPool.shutdown();
    }

    private static CompletableFuture<String> processIncomingHttpRequest(String route) {
        // Authenticate, then fetch user metrics, then merge metrics into a unified JSON body
        return fetchSecurityTokenAsync(route)
                .thenCompose(token -> fetchUserDataAsync(token))
                .thenApply(userData -> "{ \"status\": \"SUCCESS\", \"payload\": \"" + userData + "\" }");
    }

    private static CompletableFuture<String> fetchSecurityTokenAsync(String route) {
        return CompletableFuture.supplyAsync(() -> "AUTH_TOKEN_XYZ_123", proxyWorkerPool);
    }

    private static CompletableFuture<String> fetchUserDataAsync(String token) {
        return CompletableFuture.supplyAsync(() -> "UserMetadata[Mehedi, GoldTier]", proxyWorkerPool);
    }
}