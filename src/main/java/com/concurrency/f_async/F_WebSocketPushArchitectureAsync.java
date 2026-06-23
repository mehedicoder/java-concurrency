package com.concurrency.f_async;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class F_WebSocketPushArchitectureAsync {

    public static void main(String[] args) {
        ExecutorService handlingPool =
                Executors.newFixedThreadPool(4);

        try {
            WebSocketPushService pushService =
                    new WebSocketPushService(
                            handlingPool,
                            rawMessage ->
                                    "Processed JSON Payload: "
                                            + rawMessage.toUpperCase(
                                            Locale.ROOT
                                    ),
                            message -> System.out.println(
                                    "[Push Outbound] Sent to client UI -> "
                                            + message
                            ),
                            System.out::println
                    );

            String sessionId = "WS-SESSION-99X";

            CompletableFuture<String> pushResult =
                    pushService.registerSession(sessionId);

            System.out.println(
                    "[WebSocket Server] Main system continues "
                            + "processing other network events."
            );

            boolean delivered = pushService.onDataReceived(
                    sessionId,
                    "{ 'action': 'checkout', 'items': 3 }"
            );

            if (!delivered) {
                System.err.println(
                        "No active session found for " + sessionId
                );
            }

            /*
             * Console demonstration only.
             * A real server remains alive independently.
             */
            pushResult.join();

        } catch (CompletionException e) {
            System.err.println(
                    "WebSocket push failed: "
                            + rootCause(e).getMessage()
            );
        } finally {
            shutdownExecutor(handlingPool);
        }
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }

    private static void shutdownExecutor(
            ExecutorService executor
    ) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(
                    5,
                    TimeUnit.SECONDS
            )) {
                executor.shutdownNow();

                if (!executor.awaitTermination(
                        2,
                        TimeUnit.SECONDS
                )) {
                    System.err.println(
                            "WebSocket handling executor did not terminate."
                    );
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

/**
 * Coordinates registered WebSocket sessions and asynchronous push handling.
 */
final class WebSocketPushService {

    private final ConcurrentHashMap<String, CompletableFuture<String>>
            activeSessions = new ConcurrentHashMap<>();

    private final ExecutorService executor;
    private final Function<String, String> messageProcessor;
    private final Consumer<String> outboundSender;
    private final Consumer<String> logger;

    WebSocketPushService(
            ExecutorService executor,
            Function<String, String> messageProcessor,
            Consumer<String> outboundSender,
            Consumer<String> logger
    ) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.messageProcessor = Objects.requireNonNull(
                messageProcessor,
                "messageProcessor must not be null"
        );

        this.outboundSender = Objects.requireNonNull(
                outboundSender,
                "outboundSender must not be null"
        );

        this.logger = Objects.requireNonNull(
                logger,
                "logger must not be null"
        );
    }

    /**
     * Registers a new session and returns a future representing the
     * complete processing and outbound-delivery pipeline.
     */
    CompletableFuture<String> registerSession(String sessionId) {
        validateSessionId(sessionId);

        CompletableFuture<String> incomingMessage =
                new CompletableFuture<>();

        CompletableFuture<String> existing =
                activeSessions.putIfAbsent(
                        sessionId,
                        incomingMessage
                );

        if (existing != null) {
            throw new DuplicateSessionException(
                    "Session is already registered: " + sessionId
            );
        }

        logger.accept(
                "[WebSocket Server] Registered listener for session: "
                        + sessionId
        );

        CompletableFuture<String> processingPipeline =
                incomingMessage
                        .thenApplyAsync(
                                messageProcessor,
                                executor
                        )
                        .thenApply(processedMessage -> {
                            outboundSender.accept(processedMessage);
                            return processedMessage;
                        });

        /*
         * Defensive cleanup if the future is cancelled or completed
         * exceptionally by something other than onDataReceived().
         */
        processingPipeline.whenComplete(
                (result, error) ->
                        activeSessions.remove(
                                sessionId,
                                incomingMessage
                        )
        );

        return processingPipeline;
    }

    /**
     * Completes a registered session.
     *
     * @return true when an active session was found and completed;
     *         false when no matching session existed.
     */
    boolean onDataReceived(
            String sessionId,
            String rawPayload
    ) {
        validateSessionId(sessionId);
        validatePayload(rawPayload);

        CompletableFuture<String> pendingFuture =
                activeSessions.remove(sessionId);

        if (pendingFuture == null) {
            return false;
        }

        return pendingFuture.complete(rawPayload);
    }

    /**
     * Completes a session exceptionally, for example after a socket error.
     */
    boolean onSessionFailure(
            String sessionId,
            Throwable failure
    ) {
        validateSessionId(sessionId);

        Objects.requireNonNull(
                failure,
                "failure must not be null"
        );

        CompletableFuture<String> pendingFuture =
                activeSessions.remove(sessionId);

        if (pendingFuture == null) {
            return false;
        }

        return pendingFuture.completeExceptionally(failure);
    }

    /**
     * Removes and cancels an active session.
     */
    boolean disconnectSession(String sessionId) {
        validateSessionId(sessionId);

        CompletableFuture<String> pendingFuture =
                activeSessions.remove(sessionId);

        return pendingFuture != null
                && pendingFuture.cancel(false);
    }

    /**
     * Optional timeout registration.
     */
    CompletableFuture<String> registerSession(
            String sessionId,
            Duration timeout
    ) {
        Objects.requireNonNull(
                timeout,
                "timeout must not be null"
        );

        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException(
                    "Timeout must be positive."
            );
        }

        CompletableFuture<String> pipeline =
                registerSession(sessionId);

        return pipeline.orTimeout(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    int activeSessionCount() {
        return activeSessions.size();
    }

    boolean hasActiveSession(String sessionId) {
        validateSessionId(sessionId);
        return activeSessions.containsKey(sessionId);
    }

    private static void validateSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException(
                    "Session ID is required."
            );
        }
    }

    private static void validatePayload(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            throw new IllegalArgumentException(
                    "Raw payload is required."
            );
        }
    }
}

class DuplicateSessionException extends RuntimeException {

    DuplicateSessionException(String message) {
        super(message);
    }
}