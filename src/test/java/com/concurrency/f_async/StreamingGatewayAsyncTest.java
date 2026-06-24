package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StreamingGatewayAsyncTest {

    private ExecutorService executor;
    private StreamingGatewayService gateway;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(6);
        gateway = new StreamingGatewayService(executor);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        gateway.close();

        executor.shutdownNow();

        assertTrue(
                executor.awaitTermination(2, TimeUnit.SECONDS),
                "Executor did not terminate"
        );
    }

    @Test
    void constructorShouldRejectNullExecutor() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new StreamingGatewayService(null)
        );

        assertEquals(
                "executor must not be null",
                exception.getMessage()
        );
    }

    @Test
    void connectClientAsyncShouldConnectClient() {
        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "MehediTestClient"
                );

        boolean connected =
                gateway.connectClientAsync(client).join();

        assertAll(
                () -> assertTrue(connected),
                () -> assertTrue(
                        gateway.hasConnectedClient("mehedi")
                ),
                () -> assertEquals(
                        1,
                        gateway.connectedClientCount()
                ),
                () -> assertTrue(client.isConnected()),
                () -> assertTrue(client.hasSubscriber())
        );
    }

    @Test
    void connectClientAsyncShouldRejectNullClient() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> gateway.connectClientAsync(null)
        );

        assertEquals(
                "client must not be null",
                exception.getMessage()
        );
    }

    @Test
    void connectClientAsyncShouldRejectBlankUserId() {
        TestStreamingClient client =
                new TestStreamingClient(
                        " ",
                        "BlankUserClient"
                );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.connectClientAsync(client)
        );

        assertEquals(
                "User ID is required.",
                exception.getMessage()
        );
    }

    @Test
    void connectClientAsyncShouldRejectDuplicateUserConnection() {
        TestStreamingClient firstClient =
                new TestStreamingClient(
                        "mehedi",
                        "FirstClient"
                );

        TestStreamingClient secondClient =
                new TestStreamingClient(
                        "mehedi",
                        "SecondClient"
                );

        gateway.connectClientAsync(firstClient).join();

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway
                        .connectClientAsync(secondClient)
                        .join()
        );

        DuplicateClientConnectionException cause =
                assertInstanceOf(
                        DuplicateClientConnectionException.class,
                        rootCause(exception)
                );

        assertEquals(
                "A client is already connected for user: mehedi",
                cause.getMessage()
        );

        assertEquals(
                1,
                gateway.connectedClientCount()
        );

        assertTrue(firstClient.isConnected());
        assertFalse(secondClient.isConnected());
    }

    @Test
    void connectClientAsyncShouldRemoveClientWhenConnectionFails() {
        RuntimeException connectionFailure =
                new RuntimeException("Connection unavailable");

        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "FailingClient"
                );

        client.failConnectionWith(connectionFailure);

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway
                        .connectClientAsync(client)
                        .join()
        );

        assertSame(
                connectionFailure,
                rootCause(exception)
        );

        assertAll(
                () -> assertFalse(
                        gateway.hasConnectedClient("mehedi")
                ),
                () -> assertEquals(
                        0,
                        gateway.connectedClientCount()
                ),
                () -> assertTrue(client.wasClosed())
        );
    }

    @Test
    void disconnectClientAsyncShouldDisconnectExistingClient() {
        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "MehediTestClient"
                );

        gateway.connectClientAsync(client).join();

        boolean disconnected =
                gateway.disconnectClientAsync("mehedi").join();

        assertAll(
                () -> assertTrue(disconnected),
                () -> assertFalse(
                        gateway.hasConnectedClient("mehedi")
                ),
                () -> assertEquals(
                        0,
                        gateway.connectedClientCount()
                ),
                () -> assertTrue(client.wasClosed()),
                () -> assertTrue(
                        client.subscriptionWasCancelled()
                )
        );
    }

    @Test
    void disconnectClientAsyncShouldReturnFalseForUnknownUser() {
        boolean disconnected =
                gateway.disconnectClientAsync("unknown").join();

        assertFalse(disconnected);
        assertEquals(0, gateway.connectedClientCount());
    }

    @Test
    void disconnectClientAsyncShouldRejectNullUserId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.disconnectClientAsync(null)
        );

        assertEquals(
                "User ID is required.",
                exception.getMessage()
        );
    }

    @Test
    void disconnectClientAsyncShouldRejectBlankUserId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.disconnectClientAsync(" ")
        );

        assertEquals(
                "User ID is required.",
                exception.getMessage()
        );
    }

    @Test
    void hasConnectedClientShouldRejectBlankUserId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.hasConnectedClient("")
        );

        assertEquals(
                "User ID is required.",
                exception.getMessage()
        );
    }

    @Test
    void subscribeShouldRejectNullSubscriber() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> gateway.subscribe(null)
        );

        assertEquals(
                "subscriber must not be null",
                exception.getMessage()
        );
    }

    @Test
    void connectedClientCountShouldReflectConnectedClients() {
        TestStreamingClient mehedi =
                new TestStreamingClient(
                        "mehedi",
                        "MehediClient"
                );

        TestStreamingClient sarah =
                new TestStreamingClient(
                        "sarah",
                        "SarahClient"
                );

        TestStreamingClient daniel =
                new TestStreamingClient(
                        "daniel",
                        "DanielClient"
                );

        CompletableFuture.allOf(
                gateway.connectClientAsync(mehedi),
                gateway.connectClientAsync(sarah),
                gateway.connectClientAsync(daniel)
        ).join();

        assertEquals(
                3,
                gateway.connectedClientCount()
        );

        gateway.disconnectClientAsync("sarah").join();

        assertAll(
                () -> assertEquals(
                        2,
                        gateway.connectedClientCount()
                ),
                () -> assertTrue(
                        gateway.hasConnectedClient("mehedi")
                ),
                () -> assertFalse(
                        gateway.hasConnectedClient("sarah")
                ),
                () -> assertTrue(
                        gateway.hasConnectedClient("daniel")
                )
        );
    }

    @Test
    void gatewayShouldPublishProcessedResponse() throws Exception {
        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "MehediDataClient"
                );

        RecordingGatewaySubscriber subscriber =
                new RecordingGatewaySubscriber(1);

        gateway.subscribe(subscriber);
        gateway.connectClientAsync(client).join();

        client.publish(
                new UserDataEvent(
                        "mehedi",
                        1,
                        "profile-updated",
                        Instant.now()
                )
        );

        assertTrue(
                subscriber.awaitResponses(2, TimeUnit.SECONDS),
                "Gateway response was not received"
        );

        GatewayStreamResponse response =
                subscriber.responses().getFirst();

        assertAll(
                () -> assertEquals(
                        "mehedi",
                        response.userId()
                ),
                () -> assertEquals(
                        "MehediDataClient",
                        response.sourceClient()
                ),
                () -> assertEquals(
                        1,
                        response.sequenceNumber()
                ),
                () -> assertEquals(
                        "PROCESSED-PROFILE-UPDATED",
                        response.payload()
                ),
                () -> assertNotNull(response.processedAt()),
                () -> assertNull(subscriber.error())
        );
    }

    @Test
    void gatewayShouldPublishMultipleResponsesInOrder()
            throws Exception {

        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "MehediDataClient"
                );

        RecordingGatewaySubscriber subscriber =
                new RecordingGatewaySubscriber(3);

        gateway.subscribe(subscriber);
        gateway.connectClientAsync(client).join();

        client.publish(
                new UserDataEvent(
                        "mehedi",
                        1,
                        "first-event",
                        Instant.now()
                )
        );

        client.publish(
                new UserDataEvent(
                        "mehedi",
                        2,
                        "second-event",
                        Instant.now()
                )
        );

        client.publish(
                new UserDataEvent(
                        "mehedi",
                        3,
                        "third-event",
                        Instant.now()
                )
        );

        assertTrue(
                subscriber.awaitResponses(2, TimeUnit.SECONDS),
                "Not all gateway responses were received"
        );

        List<GatewayStreamResponse> responses =
                subscriber.responses();

        assertEquals(3, responses.size());

        assertEquals(
                List.of(1L, 2L, 3L),
                responses.stream()
                        .map(
                                GatewayStreamResponse::
                                        sequenceNumber
                        )
                        .toList()
        );

        assertEquals(
                List.of(
                        "PROCESSED-FIRST-EVENT",
                        "PROCESSED-SECOND-EVENT",
                        "PROCESSED-THIRD-EVENT"
                ),
                responses.stream()
                        .map(GatewayStreamResponse::payload)
                        .toList()
        );
    }

    @Test
    void gatewayShouldHandleEventsFromMultipleClients()
            throws Exception {

        TestStreamingClient mehediClient =
                new TestStreamingClient(
                        "mehedi",
                        "MehediDataClient"
                );

        TestStreamingClient sarahClient =
                new TestStreamingClient(
                        "sarah",
                        "SarahDataClient"
                );

        RecordingGatewaySubscriber subscriber =
                new RecordingGatewaySubscriber(2);

        gateway.subscribe(subscriber);

        CompletableFuture.allOf(
                gateway.connectClientAsync(mehediClient),
                gateway.connectClientAsync(sarahClient)
        ).join();

        mehediClient.publish(
                new UserDataEvent(
                        "mehedi",
                        1,
                        "profile-updated",
                        Instant.now()
                )
        );

        sarahClient.publish(
                new UserDataEvent(
                        "sarah",
                        1,
                        "payment-completed",
                        Instant.now()
                )
        );

        assertTrue(
                subscriber.awaitResponses(2, TimeUnit.SECONDS),
                "Responses from both clients were not received"
        );

        assertEquals(
                List.of("mehedi", "sarah"),
                subscriber.responses()
                        .stream()
                        .map(GatewayStreamResponse::userId)
                        .sorted()
                        .toList()
        );
    }

    @Test
    void gatewayShouldCloseExceptionallyForUnexpectedUserEvent()
            throws Exception {

        TestStreamingClient client =
                new TestStreamingClient(
                        "mehedi",
                        "MehediDataClient"
                );

        RecordingGatewaySubscriber subscriber =
                new RecordingGatewaySubscriber(0);

        gateway.subscribe(subscriber);
        gateway.connectClientAsync(client).join();

        client.publish(
                new UserDataEvent(
                        "sarah",
                        1,
                        "invalid-user-event",
                        Instant.now()
                )
        );

        assertTrue(
                subscriber.awaitTerminalSignal(
                        2,
                        TimeUnit.SECONDS
                ),
                "Expected exceptional stream termination"
        );

        IllegalStateException error =
                assertInstanceOf(
                        IllegalStateException.class,
                        subscriber.error()
                );

        assertEquals(
                "MehediDataClient emitted data "
                        + "for unexpected user: sarah",
                error.getMessage()
        );

        assertTrue(
                client.subscriptionWasCancelled()
        );
    }

    @Test
    void closeShouldCloseAllConnectedClients() {
        TestStreamingClient mehedi =
                new TestStreamingClient(
                        "mehedi",
                        "MehediClient"
                );

        TestStreamingClient sarah =
                new TestStreamingClient(
                        "sarah",
                        "SarahClient"
                );

        gateway.connectClientAsync(mehedi).join();
        gateway.connectClientAsync(sarah).join();

        gateway.close();

        assertAll(
                () -> assertEquals(
                        0,
                        gateway.connectedClientCount()
                ),
                () -> assertTrue(mehedi.wasClosed()),
                () -> assertTrue(sarah.wasClosed()),
                () -> assertTrue(
                        mehedi.subscriptionWasCancelled()
                ),
                () -> assertTrue(
                        sarah.subscriptionWasCancelled()
                )
        );
    }

    @Test
    void externalClientShouldConnectAndPublishEvent()
            throws Exception {

        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        RecordingUserEventSubscriber subscriber =
                new RecordingUserEventSubscriber(1);

        client.subscribe(subscriber);
        client.connectAsync().join();

        client.onExternalDataReceived("profile-updated");

        assertTrue(
                subscriber.awaitEvents(2, TimeUnit.SECONDS),
                "User event was not received"
        );

        UserDataEvent event =
                subscriber.events().getFirst();

        assertAll(
                () -> assertEquals(
                        "mehedi",
                        event.userId()
                ),
                () -> assertEquals(
                        1,
                        event.sequenceNumber()
                ),
                () -> assertEquals(
                        "profile-updated",
                        event.payload()
                ),
                () -> assertNotNull(event.occurredAt())
        );

        client.close();
    }

    @Test
    void externalClientShouldIncrementSequenceNumbers()
            throws Exception {

        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        RecordingUserEventSubscriber subscriber =
                new RecordingUserEventSubscriber(3);

        client.subscribe(subscriber);
        client.connectAsync().join();

        client.onExternalDataReceived("event-one");
        client.onExternalDataReceived("event-two");
        client.onExternalDataReceived("event-three");

        assertTrue(
                subscriber.awaitEvents(2, TimeUnit.SECONDS),
                "Not all user events were received"
        );

        assertEquals(
                List.of(1L, 2L, 3L),
                subscriber.events()
                        .stream()
                        .map(UserDataEvent::sequenceNumber)
                        .toList()
        );

        client.close();
    }

    @Test
    void externalClientShouldRejectPublishingBeforeConnection() {
        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> client.onExternalDataReceived(
                        "profile-updated"
                )
        );

        assertEquals(
                "MehediDataClient is not connected.",
                exception.getMessage()
        );
    }

    @Test
    void externalClientShouldRejectBlankPayload() {
        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        client.connectAsync().join();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> client.onExternalDataReceived(" ")
        );

        assertEquals(
                "External payload is required.",
                exception.getMessage()
        );

        client.close();
    }

    @Test
    void externalClientShouldRejectNullPayload() {
        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        client.connectAsync().join();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> client.onExternalDataReceived(null)
        );

        assertEquals(
                "External payload is required.",
                exception.getMessage()
        );

        client.close();
    }

    @Test
    void externalClientShouldRejectSecondConnection() {
        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        client.connectAsync().join();

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> client.connectAsync().join()
        );

        IllegalStateException cause =
                assertInstanceOf(
                        IllegalStateException.class,
                        rootCause(exception)
                );

        assertEquals(
                "MehediDataClient is already connected.",
                cause.getMessage()
        );

        client.close();
    }

    @Test
    void externalClientShouldRejectEventAfterClose() {
        ExternalEventUserDataClient client =
                new ExternalEventUserDataClient(
                        "mehedi",
                        "MehediDataClient",
                        executor
                );

        client.connectAsync().join();
        client.close();

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> client.onExternalDataReceived(
                        "event-after-close"
                )
        );

        assertEquals(
                "MehediDataClient is not connected.",
                exception.getMessage()
        );
    }

    @Test
    void externalClientConstructorShouldRejectBlankUserId() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ExternalEventUserDataClient(
                        " ",
                        "TestClient",
                        executor
                )
        );

        assertEquals(
                "userId is required.",
                exception.getMessage()
        );
    }

    @Test
    void externalClientConstructorShouldRejectBlankClientName() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ExternalEventUserDataClient(
                        "mehedi",
                        "",
                        executor
                )
        );

        assertEquals(
                "clientName is required.",
                exception.getMessage()
        );
    }

    @Test
    void externalClientConstructorShouldRejectNullExecutor() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new ExternalEventUserDataClient(
                        "mehedi",
                        "TestClient",
                        null
                )
        );

        assertEquals(
                "executor must not be null",
                exception.getMessage()
        );
    }

    @Test
    void userDataEventShouldRejectNullUserId() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new UserDataEvent(
                        null,
                        1,
                        "payload",
                        Instant.now()
                )
        );

        assertEquals(
                "userId must not be null",
                exception.getMessage()
        );
    }

    @Test
    void userDataEventShouldRejectNullPayload() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new UserDataEvent(
                        "mehedi",
                        1,
                        null,
                        Instant.now()
                )
        );

        assertEquals(
                "payload must not be null",
                exception.getMessage()
        );
    }

    @Test
    void gatewayStreamResponseShouldRejectNullProcessedAt() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayStreamResponse(
                        "mehedi",
                        "MehediClient",
                        1,
                        "payload",
                        null
                )
        );

        assertEquals(
                "processedAt must not be null",
                exception.getMessage()
        );
    }

    @Test
    void clientConnectionShouldRejectNullClient() {
        TestSubscription subscription =
                new TestSubscription();

        SubmissionPublisher<GatewayStreamResponse> publisher =
                new SubmissionPublisher<>(executor, 10);

        UserEventSubscriber subscriber =
                new UserEventSubscriber(
                        "mehedi",
                        "MehediClient",
                        publisher
                );

        subscriber.onSubscribe(subscription);

        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new ClientConnection(
                        null,
                        subscriber
                )
        );

        assertEquals(
                "client must not be null",
                exception.getMessage()
        );

        publisher.close();
    }

    private static Throwable rootCause(
            Throwable throwable
    ) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }

    private static final class RecordingGatewaySubscriber
            implements Flow.Subscriber<GatewayStreamResponse> {

        private final List<GatewayStreamResponse> responses =
                new CopyOnWriteArrayList<>();

        private final CountDownLatch responseLatch;
        private final CountDownLatch terminalLatch =
                new CountDownLatch(1);

        private final AtomicReference<Throwable> error =
                new AtomicReference<>();

        private volatile Flow.Subscription subscription;

        private RecordingGatewaySubscriber(
                int expectedResponseCount
        ) {
            this.responseLatch =
                    new CountDownLatch(expectedResponseCount);
        }

        @Override
        public void onSubscribe(
                Flow.Subscription subscription
        ) {
            this.subscription =
                    Objects.requireNonNull(subscription);

            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(
                GatewayStreamResponse response
        ) {
            responses.add(response);
            responseLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
            terminalLatch.countDown();
        }

        @Override
        public void onComplete() {
            terminalLatch.countDown();
        }

        boolean awaitResponses(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {
            return responseLatch.await(timeout, unit);
        }

        boolean awaitTerminalSignal(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {
            return terminalLatch.await(timeout, unit);
        }

        List<GatewayStreamResponse> responses() {
            return List.copyOf(responses);
        }

        Throwable error() {
            return error.get();
        }
    }

    private static final class RecordingUserEventSubscriber
            implements Flow.Subscriber<UserDataEvent> {

        private final List<UserDataEvent> events =
                new CopyOnWriteArrayList<>();

        private final CountDownLatch eventLatch;

        private RecordingUserEventSubscriber(
                int expectedEventCount
        ) {
            this.eventLatch =
                    new CountDownLatch(expectedEventCount);
        }

        @Override
        public void onSubscribe(
                Flow.Subscription subscription
        ) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(UserDataEvent event) {
            events.add(event);
            eventLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            // Not needed in these tests.
        }

        @Override
        public void onComplete() {
            // Not needed in these tests.
        }

        boolean awaitEvents(
                long timeout,
                TimeUnit unit
        ) throws InterruptedException {
            return eventLatch.await(timeout, unit);
        }

        List<UserDataEvent> events() {
            return List.copyOf(events);
        }
    }

    private static final class TestStreamingClient
            implements StreamingUserDataClient {

        private final String userId;
        private final String clientName;

        private final AtomicBoolean connected =
                new AtomicBoolean();

        private final AtomicBoolean closed =
                new AtomicBoolean();

        private final AtomicReference<Flow.Subscriber<
                ? super UserDataEvent>> subscriber =
                new AtomicReference<>();

        private final TestSubscription subscription =
                new TestSubscription();

        private volatile RuntimeException connectionFailure;

        private TestStreamingClient(
                String userId,
                String clientName
        ) {
            this.userId = userId;
            this.clientName = clientName;
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
        public CompletableFuture<Void> connectAsync() {
            if (connectionFailure != null) {
                return CompletableFuture.failedFuture(
                        connectionFailure
                );
            }

            connected.set(true);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void subscribe(
                Flow.Subscriber<? super UserDataEvent>
                        subscriber
        ) {
            this.subscriber.set(subscriber);
            subscriber.onSubscribe(subscription);
        }

        @Override
        public void close() {
            connected.set(false);
            closed.set(true);
        }

        void publish(UserDataEvent event) {
            Flow.Subscriber<? super UserDataEvent> current =
                    subscriber.get();

            if (current == null) {
                throw new IllegalStateException(
                        "No subscriber registered"
                );
            }

            current.onNext(event);
        }

        void failConnectionWith(
                RuntimeException exception
        ) {
            this.connectionFailure = exception;
        }

        boolean isConnected() {
            return connected.get();
        }

        boolean wasClosed() {
            return closed.get();
        }

        boolean hasSubscriber() {
            return subscriber.get() != null;
        }

        boolean subscriptionWasCancelled() {
            return subscription.isCancelled();
        }
    }

    private static final class TestSubscription
            implements Flow.Subscription {

        private final AtomicBoolean cancelled =
                new AtomicBoolean();

        private final AtomicLong requested =
                new AtomicLong();

        @Override
        public void request(long amount) {
            requested.addAndGet(amount);
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        boolean isCancelled() {
            return cancelled.get();
        }

        long requestedCount() {
            return requested.get();
        }
    }
}