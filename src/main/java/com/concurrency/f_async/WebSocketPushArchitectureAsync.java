package com.concurrency.f_async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WebSocketPushArchitectureAsync {

    // A register mapping unique connection IDs to active processing receipts
    private static final ConcurrentHashMap<String, CompletableFuture<String>> activeSessions = new ConcurrentHashMap<>();
    private static final ExecutorService handlingPool = Executors.newFixedThreadPool(4);

    public static void main(String[] args) throws Exception {
        String sessionId = "WS-SESSION-99X";

        // 1. Client establishes connection and registers interest
        CompletableFuture<String> messagePromise = new CompletableFuture<>();
        activeSessions.put(sessionId, messagePromise);
        System.out.println("[WebSocket Server] Registered listener for session: " + sessionId);

        // 2. Attach a functional PUSH callback to process data the split-second it arrives
        messagePromise
                .thenApplyAsync(rawMessage -> "Processed JSON Payload: " + rawMessage.toUpperCase(), handlingPool)
                .thenAccept(finalResult -> System.out.format("[Push Outbound] Sent to client UI -> %s\n", finalResult));

        System.out.println("[WebSocket Server] Main system continuing to poll other global network loops...\n");
        Thread.sleep(1500); // Simulate network quiet period

        // 3. Simulated Event Trigger: Data physically hits the network cards out of nowhere
        System.out.println("\n[Network Event Interrupt] Packet arrived on kernel buffer!");
        onDataReceived(sessionId, "{ 'action': 'checkout', 'items': 3 }");

        Thread.sleep(500); // Allow callback printing thread a brief moment to catch up
        handlingPool.shutdown();
    }

    // Bridge method called directly by underlying hardware event drivers
    public static void onDataReceived(String sessionId, String rawPayload) {
        CompletableFuture<String> pendingFuture = activeSessions.remove(sessionId);
        if (pendingFuture != null) {
            // Manually resolving the future triggers the entire push-chain automatically
            pendingFuture.complete(rawPayload);
        }
    }
}