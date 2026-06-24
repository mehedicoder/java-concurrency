package com.concurrency.f_async;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class E_StreamingGatewayAsync {

    public static void main(String[] args) {
        ExecutorService executor =
                Executors.newFixedThreadPool(8);

        StreamingGatewayService gateway =
                new StreamingGatewayService(executor);

        ExternalEventUserDataClient mehediClient =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        ExternalEventUserDataClient sarahClient =
                new ExternalEventUserDataClient(
                        "sarah",
                        "SarahDataClient",
                        executor
                );

        ExternalEventUserDataClient danielClient =
                new ExternalEventUserDataClient(
                        "daniel",
                        "DanielDataClient",
                        executor
                );

        try {
            gateway.subscribe(
                    new GatewayResponseSubscriber()
            );

            log("Gateway initialized with zero clients.");

            CompletableFuture.allOf(
                    gateway.connectClientAsync(mehediClient),
                    gateway.connectClientAsync(sarahClient),
                    gateway.connectClientAsync(danielClient)
            ).join();

            log(
                    "Connected clients: "
                            + gateway.connectedClientCount()
            );

            /*
             * Simulated external events.
             *
             * In a real system, these calls could come from:
             * - a WebSocket callback;
             * - a Kafka consumer;
             * - a message broker listener;
             * - a sensor callback;
             * - an HTTP webhook.
             */
            CompletableFuture<Void> externalEvents =
                    CompletableFuture.runAsync(
                            () -> {
                                sleep(Duration.ofMillis(500));

                                mehediClient.onExternalDataReceived(
                                        "profile-updated: GoldTier"
                                );

                                sleep(Duration.ofMillis(300));

                                sarahClient.onExternalDataReceived(
                                        "payment-completed: ORDER-202"
                                );

                                sleep(Duration.ofMillis(300));

                                danielClient.onExternalDataReceived(
                                        "login-detected: Berlin"
                                );

                                sleep(Duration.ofMillis(300));

                                mehediClient.onExternalDataReceived(
                                        "checkout-completed: ORDER-101"
                                );

                                sleep(Duration.ofMillis(300));

                                sarahClient.onExternalDataReceived(
                                        "subscription-renewed"
                                );
                            },
                            executor
                    );

            externalEvents.join();

            /*
             * Give asynchronous subscribers enough time
             * to process the submitted events.
             */
            sleep(Duration.ofSeconds(1));

            gateway.disconnectClientAsync("sarah").join();

            log(
                    "Sarah disconnected. Connected clients: "
                            + gateway.connectedClientCount()
            );

            /*
             * Sarah can no longer publish after disconnection.
             */
            try {
                sarahClient.onExternalDataReceived(
                        "event-after-disconnect"
                );
            } catch (IllegalStateException exception) {
                log(
                        "Rejected Sarah event: "
                                + exception.getMessage()
                );
            }

            /*
             * Mehedi and Daniel remain connected.
             */
            mehediClient.onExternalDataReceived(
                    "profile-viewed"
            );

            danielClient.onExternalDataReceived(
                    "logout-detected"
            );

            sleep(Duration.ofSeconds(1));

        } finally {
            gateway.close();
            shutdownExecutor(executor);
        }
    }

    private static void log(String message) {
        System.out.printf(
                "%-25s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CompletionException(exception);
        }
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
            }
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

final class StreamingGatewayService
        implements Flow.Publisher<GatewayStreamResponse>,
        AutoCloseable {

    private final ExecutorService executor;

    private final Map<String, ClientConnection>
            connectedClients =
            new ConcurrentHashMap<>();

    private final SubmissionPublisher<GatewayStreamResponse>
            outboundPublisher;

    StreamingGatewayService(ExecutorService executor) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.outboundPublisher =
                new SubmissionPublisher<>(
                        executor,
                        Flow.defaultBufferSize()
                );
    }

    CompletableFuture<Boolean> connectClientAsync(
            StreamingUserDataClient client
    ) {
        Objects.requireNonNull(
                client,
                "client must not be null"
        );

        String userId =
                requireUserId(client.userId());

        UserEventSubscriber subscriber =
                new UserEventSubscriber(
                        userId,
                        client.clientName(),
                        outboundPublisher
                );

        ClientConnection connection =
                new ClientConnection(
                        client,
                        subscriber
                );

        ClientConnection existing =
                connectedClients.putIfAbsent(
                        userId,
                        connection
                );

        if (existing != null) {
            return CompletableFuture.failedFuture(
                    new DuplicateClientConnectionException(
                            "A client is already connected for user: "
                                    + userId
                    )
            );
        }

        /*
         * Subscribe before opening the connection,
         * so the first external event is not missed.
         */
        client.subscribe(subscriber);

        return client.connectAsync()
                .thenApply(ignored -> {
                    log(
                            client.clientName()
                                    + " connected for user "
                                    + userId
                    );

                    return true;
                })
                .whenComplete((connected, error) -> {
                    if (error != null) {
                        connectedClients.remove(
                                userId,
                                connection
                        );

                        client.close();
                    }
                });
    }

    CompletableFuture<Boolean> disconnectClientAsync(
            String userId
    ) {
        requireUserId(userId);

        ClientConnection connection =
                connectedClients.remove(userId);

        if (connection == null) {
            return CompletableFuture.completedFuture(false);
        }

        return CompletableFuture.supplyAsync(
                () -> {
                    connection.subscriber().cancel();
                    connection.client().close();

                    log(
                            "Disconnected client for user "
                                    + userId
                    );

                    return true;
                },
                executor
        );
    }

    boolean hasConnectedClient(String userId) {
        requireUserId(userId);

        return connectedClients.containsKey(userId);
    }

    int connectedClientCount() {
        return connectedClients.size();
    }

    @Override
    public void subscribe(
            Flow.Subscriber<? super GatewayStreamResponse>
                    subscriber
    ) {
        outboundPublisher.subscribe(
                Objects.requireNonNull(
                        subscriber,
                        "subscriber must not be null"
                )
        );
    }

    @Override
    public void close() {
        connectedClients.forEach(
                (userId, connection) -> {
                    connection.subscriber().cancel();
                    connection.client().close();
                }
        );

        connectedClients.clear();
        outboundPublisher.close();
    }

    private static String requireUserId(
            String userId
    ) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException(
                    "User ID is required."
            );
        }

        return userId;
    }

    private static void log(String message) {
        System.out.printf(
                "%-25s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }
}

final class UserEventSubscriber
        implements Flow.Subscriber<UserDataEvent> {

    private final String expectedUserId;
    private final String clientName;

    private final SubmissionPublisher<GatewayStreamResponse>
            outboundPublisher;

    private volatile Flow.Subscription subscription;

    UserEventSubscriber(
            String expectedUserId,
            String clientName,
            SubmissionPublisher<GatewayStreamResponse>
                    outboundPublisher
    ) {
        this.expectedUserId =
                Objects.requireNonNull(expectedUserId);

        this.clientName =
                Objects.requireNonNull(clientName);

        this.outboundPublisher =
                Objects.requireNonNull(outboundPublisher);
    }

    @Override
    public void onSubscribe(
            Flow.Subscription subscription
    ) {
        this.subscription =
                Objects.requireNonNull(subscription);

        /*
         * Request the first event.
         */
        subscription.request(1);
    }

    @Override
    public void onNext(UserDataEvent event) {
        try {
            if (!expectedUserId.equals(event.userId())) {
                throw new IllegalStateException(
                        clientName
                                + " emitted data for unexpected user: "
                                + event.userId()
                );
            }

            GatewayStreamResponse response =
                    new GatewayStreamResponse(
                            event.userId(),
                            clientName,
                            event.sequenceNumber(),
                            "PROCESSED-"
                                    + event.payload().toUpperCase(),
                            Instant.now()
                    );

            outboundPublisher.submit(response);

        } catch (RuntimeException exception) {
            cancel();
            outboundPublisher.closeExceptionally(
                    exception
            );

            return;
        }

        /*
         * Request the next event only after this event
         * has been processed.
         */
        Flow.Subscription current =
                subscription;

        if (current != null) {
            current.request(1);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println(
                clientName
                        + " stream failed: "
                        + throwable.getMessage()
        );
    }

    @Override
    public void onComplete() {
        System.out.println(
                clientName + " stream completed."
        );
    }

    void cancel() {
        Flow.Subscription current =
                subscription;

        if (current != null) {
            current.cancel();
        }
    }
}

interface StreamingUserDataClient
        extends Flow.Publisher<UserDataEvent>,
        AutoCloseable {

    String userId();

    String clientName();

    CompletableFuture<Void> connectAsync();

    @Override
    void close();
}

final class ExternalEventUserDataClient
        implements StreamingUserDataClient {

    private final String userId;
    private final String clientName;

    private final SubmissionPublisher<UserDataEvent>
            publisher;

    private final AtomicBoolean connected =
            new AtomicBoolean(false);

    private final AtomicLong sequence =
            new AtomicLong();

    ExternalEventUserDataClient(
            String userId,
            String clientName,
            ExecutorService executor
    ) {
        this.userId =
                requireText(
                        userId,
                        "userId"
                );

        this.clientName =
                requireText(
                        clientName,
                        "clientName"
                );

        this.publisher =
                new SubmissionPublisher<>(
                        Objects.requireNonNull(
                                executor,
                                "executor must not be null"
                        ),
                        Flow.defaultBufferSize()
                );
    }

    @Override
    public String userId() {
        return userId;
    }

    @Override
    public String clientName() {
        return clientName;
    }

    @Override
    public void subscribe(
            Flow.Subscriber<? super UserDataEvent>
                    subscriber
    ) {
        publisher.subscribe(
                Objects.requireNonNull(
                        subscriber,
                        "subscriber must not be null"
                )
        );
    }

    /**
     * Establishes the connection only.
     *
     * It does not start an event-generation loop.
     */
    @Override
    public CompletableFuture<Void> connectAsync() {
        if (!connected.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            clientName
                                    + " is already connected."
                    )
            );
        }

        log(
                clientName
                        + " connection established for "
                        + userId
        );

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Called by an external source when real data arrives.
     */
    void onExternalDataReceived(String payload) {
        if (!connected.get()) {
            throw new IllegalStateException(
                    clientName
                            + " is not connected."
            );
        }

        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException(
                    "External payload is required."
            );
        }

        long eventSequence =
                sequence.incrementAndGet();

        UserDataEvent event =
                new UserDataEvent(
                        userId,
                        eventSequence,
                        payload,
                        Instant.now()
                );

        int lag = publisher.submit(event);

        log(
                clientName
                        + " published external event "
                        + eventSequence
                        + " for "
                        + userId
                        + "; estimated maximum lag="
                        + lag
        );
    }

    @Override
    public void close() {
        if (connected.compareAndSet(true, false)) {
            publisher.close();

            log(
                    clientName
                            + " connection closed for "
                            + userId
            );
        }
    }

    private static String requireText(
            String value,
            String fieldName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " is required."
            );
        }

        return value;
    }

    private static void log(String message) {
        System.out.printf(
                "%-25s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }
}

final class GatewayResponseSubscriber
        implements Flow.Subscriber<GatewayStreamResponse> {

    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(
            Flow.Subscription subscription
    ) {
        this.subscription =
                Objects.requireNonNull(subscription);

        subscription.request(1);
    }

    @Override
    public void onNext(
            GatewayStreamResponse response
    ) {
        System.out.printf(
                "%-25s Gateway response: "
                        + "user=%s, client=%s, sequence=%d, payload=%s%n",
                "[" + Thread.currentThread().getName() + "]",
                response.userId(),
                response.sourceClient(),
                response.sequenceNumber(),
                response.payload()
        );

        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println(
                "Gateway output stream failed: "
                        + throwable.getMessage()
        );
    }

    @Override
    public void onComplete() {
        System.out.println(
                "Gateway output stream completed."
        );
    }
}

record ClientConnection(
        StreamingUserDataClient client,
        UserEventSubscriber subscriber
) {
    ClientConnection {
        Objects.requireNonNull(
                client,
                "client must not be null"
        );

        Objects.requireNonNull(
                subscriber,
                "subscriber must not be null"
        );
    }
}

record UserDataEvent(
        String userId,
        long sequenceNumber,
        String payload,
        Instant occurredAt
) {
    UserDataEvent {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        Objects.requireNonNull(
                payload,
                "payload must not be null"
        );

        Objects.requireNonNull(
                occurredAt,
                "occurredAt must not be null"
        );
    }
}

record GatewayStreamResponse(
        String userId,
        String sourceClient,
        long sequenceNumber,
        String payload,
        Instant processedAt
) {
    GatewayStreamResponse {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        Objects.requireNonNull(
                sourceClient,
                "sourceClient must not be null"
        );

        Objects.requireNonNull(
                payload,
                "payload must not be null"
        );

        Objects.requireNonNull(
                processedAt,
                "processedAt must not be null"
        );
    }
}

class DuplicateClientConnectionException
        extends RuntimeException {

    DuplicateClientConnectionException(
            String message
    ) {
        super(message);
    }
}