package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class WebSocketPushArchitectureAsyncTest {

    private ExecutorService executor;
    private List<String> outboundMessages;
    private List<String> logs;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
        outboundMessages = new CopyOnWriteArrayList<>();
        logs = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();

        try {
            assertTrue(
                    executor.awaitTermination(2, TimeUnit.SECONDS),
                    "The WebSocket executor failed to terminate."
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            fail(
                    "The test thread was interrupted while shutting down the executor.",
                    e
            );
        }
    }

    @Test
    @DisplayName(
            "Verify that an incoming WebSocket message is processed and delivered successfully"
    )
    void testIncomingMessageIsProcessedAndDelivered() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-001");

        // Act
        boolean delivered = pushService.onDataReceived(
                "SESSION-001",
                "{ \"action\": \"checkout\" }"
        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        String result = resultFuture.join();

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertFalse(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertFalse(resultFuture.isCancelled()),
                () -> assertEquals(
                        "Processed JSON Payload: "
                                + "{ \"ACTION\": \"CHECKOUT\" }",
                        result
                ),
                () -> assertEquals(1, outboundMessages.size()),
                () -> assertEquals(
                        result,
                        outboundMessages.getFirst()
                ),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that registering a session adds it to the active-session registry"
    )
    void testRegisterSessionAddsActiveSession() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-002");

        // Assert
        assertAll(
                () -> assertNotNull(resultFuture),
                () -> assertFalse(resultFuture.isDone()),
                () -> assertTrue(
                        pushService.hasActiveSession("SESSION-002")
                ),
                () -> assertEquals(
                        1,
                        pushService.activeSessionCount()
                ),
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message -> message.contains(
                                        "Registered listener for session: SESSION-002"
                                )
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that message processing runs asynchronously on an executor worker"
    )
    void testMessageProcessingRunsOnWorkerThread() {
        // Arrange
        String testThreadName =
                Thread.currentThread().getName();

        AtomicReference<String> processingThreadName =
                new AtomicReference<>();

        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        rawMessage -> {
                            processingThreadName.set(
                                    Thread.currentThread().getName()
                            );
                            return rawMessage.toUpperCase();
                        },
                        outboundMessages::add,
                        logs::add
                );

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-003");

        // Act
        pushService.onDataReceived(
                "SESSION-003",
                "hello"
        );

        resultFuture.join();

        // Assert
        assertAll(
                () -> assertNotNull(processingThreadName.get()),
                () -> assertNotEquals(
                        testThreadName,
                        processingThreadName.get()
                ),
                () -> assertEquals(
                        "HELLO",
                        resultFuture.join()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that the raw payload is passed unchanged to the message processor"
    )
    void testRawPayloadIsPassedToMessageProcessor() {
        // Arrange
        AtomicReference<String> receivedPayload =
                new AtomicReference<>();

        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        rawPayload -> {
                            receivedPayload.set(rawPayload);
                            return "PROCESSED";
                        },
                        outboundMessages::add,
                        logs::add
                );

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-004");

        String rawPayload =
                "{ \"type\": \"payment\", \"amount\": 49.99 }";

        // Act
        pushService.onDataReceived(
                "SESSION-004",
                rawPayload
        );

        resultFuture.join();

        // Assert
        assertEquals(
                rawPayload,
                receivedPayload.get()
        );
    }

    @Test
    @DisplayName(
            "Verify that the processed message is passed to the outbound sender"
    )
    void testProcessedMessageIsPassedToOutboundSender() {
        // Arrange
        AtomicReference<String> deliveredMessage =
                new AtomicReference<>();

        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        rawPayload -> "TRANSFORMED-" + rawPayload,
                        deliveredMessage::set,
                        logs::add
                );

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-005");

        // Act
        pushService.onDataReceived(
                "SESSION-005",
                "MESSAGE"
        );

        String result = resultFuture.join();

        // Assert
        assertAll(
                () -> assertEquals(
                        "TRANSFORMED-MESSAGE",
                        result
                ),
                () -> assertEquals(
                        "TRANSFORMED-MESSAGE",
                        deliveredMessage.get()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that only the first event is delivered for a one-shot session"
    )
    void testOnlyFirstMessageIsDelivered() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-006");

        // Act
        boolean firstDelivery = pushService.onDataReceived(
                "SESSION-006",
                "first-message"
        );

        boolean secondDelivery = pushService.onDataReceived(
                "SESSION-006",
                "second-message"
        );

        String result = resultFuture.join();

        // Assert
        assertAll(
                () -> assertTrue(firstDelivery),
                () -> assertFalse(secondDelivery),
                () -> assertEquals(
                        "Processed JSON Payload: FIRST-MESSAGE",
                        result
                ),
                () -> assertEquals(1, outboundMessages.size())
        );
    }

    @Test
    @DisplayName(
            "Verify that an event for an unknown session returns false"
    )
    void testUnknownSessionReturnsFalse() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        boolean delivered = pushService.onDataReceived(
                "UNKNOWN-SESSION",
                "message"
        );

        // Assert
        assertAll(
                () -> assertFalse(delivered),
                () -> assertTrue(outboundMessages.isEmpty()),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that duplicate session registration is rejected"
    )
    void testDuplicateSessionRegistrationIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        pushService.registerSession("SESSION-007");

        // Act
        DuplicateSessionException exception = assertThrows(
                DuplicateSessionException.class,
                () -> pushService.registerSession("SESSION-007")
        );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Session is already registered: SESSION-007",
                        exception.getMessage()
                ),
                () -> assertEquals(
                        1,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that message processor failure completes the pipeline exceptionally"
    )
    void testMessageProcessorFailureCompletesPipelineExceptionally() {
        // Arrange
        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        rawMessage -> {
                            throw new IllegalStateException(
                                    "Invalid WebSocket payload."
                            );
                        },
                        outboundMessages::add,
                        logs::add
                );

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-008");

        // Act
        boolean delivered = pushService.onDataReceived(
                "SESSION-008",
                "bad-message"
        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        Throwable cause = rootCause(exception);

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "Invalid WebSocket payload.",
                        cause.getMessage()
                ),
                () -> assertTrue(outboundMessages.isEmpty()),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that outbound sender failure completes the pipeline exceptionally"
    )
    void testOutboundSenderFailureCompletesPipelineExceptionally() {
        // Arrange
        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        String::toUpperCase,
                        message -> {
                            throw new IllegalStateException(
                                    "WebSocket connection was closed."
                            );
                        },
                        logs::add
                );

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-009");

        // Act
        pushService.onDataReceived(
                "SESSION-009",
                "hello"
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        Throwable cause = rootCause(exception);

        // Assert
        assertAll(
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "WebSocket connection was closed.",
                        cause.getMessage()
                ),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a socket failure completes the session exceptionally"
    )
    void testSessionFailureCompletesPipelineExceptionally() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-010");

        RuntimeException socketFailure =
                new RuntimeException("Remote peer disconnected.");

        // Act
        boolean completed =
                pushService.onSessionFailure(
                        "SESSION-010",
                        socketFailure
                );

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(completed),
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertSame(
                        socketFailure,
                        rootCause(exception)
                ),
                () -> assertTrue(outboundMessages.isEmpty()),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that failure notification for an unknown session returns false"
    )
    void testFailureForUnknownSessionReturnsFalse() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        boolean completed = pushService.onSessionFailure(
                "UNKNOWN-SESSION",
                new RuntimeException("Connection error.")
        );

        // Assert
        assertFalse(completed);
    }

    @Test
    @DisplayName(
            "Verify that disconnecting an active session completes its pipeline exceptionally with cancellation"
    )
    void testDisconnectSessionCancelsPipeline() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-011");

        // Act
        boolean disconnected =
                pushService.disconnectSession("SESSION-011");

        await()
                .atMost(Duration.ofSeconds(1))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(disconnected),
                () -> assertTrue(resultFuture.isDone()),
                () -> assertFalse(
                        resultFuture.isCancelled(),
                        "Only the internal source future was directly cancelled."
                ),
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        CancellationException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that disconnecting an unknown session returns false"
    )
    void testDisconnectUnknownSessionReturnsFalse() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        boolean disconnected =
                pushService.disconnectSession("UNKNOWN-SESSION");

        // Assert
        assertFalse(disconnected);
    }

    @Test
    @DisplayName(
            "Verify that session timeout completes the pipeline exceptionally"
    )
    void testSessionTimeoutCompletesPipelineExceptionally() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        CompletableFuture<String> resultFuture =
                pushService.registerSession(
                        "SESSION-012",
                        Duration.ofMillis(100)
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        TimeoutException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        0,
                        pushService.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that data received before timeout completes successfully"
    )
    void testMessageBeforeTimeoutCompletesSuccessfully() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession(
                        "SESSION-013",
                        Duration.ofSeconds(1)
                );

        // Act
        boolean delivered = pushService.onDataReceived(
                "SESSION-013",
                "within-deadline"
        );

        String result = resultFuture.join();

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertFalse(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        "Processed JSON Payload: WITHIN-DEADLINE",
                        result
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that multiple sessions can be processed concurrently"
    )
    void testMultipleSessionsAreProcessedConcurrently() {
        // Arrange
        CountDownLatch bothProcessorsStarted =
                new CountDownLatch(2);

        CountDownLatch releaseProcessors =
                new CountDownLatch(1);

        AtomicInteger activeProcessors =
                new AtomicInteger();

        AtomicInteger maximumConcurrency =
                new AtomicInteger();

        WebSocketPushService pushService =
                new WebSocketPushService(
                        executor,
                        message -> {
                            int active =
                                    activeProcessors.incrementAndGet();

                            maximumConcurrency.accumulateAndGet(
                                    active,
                                    Math::max
                            );

                            bothProcessorsStarted.countDown();

                            try {
                                releaseProcessors.await();
                                return message.toUpperCase();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(e);
                            } finally {
                                activeProcessors.decrementAndGet();
                            }
                        },
                        outboundMessages::add,
                        logs::add
                );

        CompletableFuture<String> firstFuture =
                pushService.registerSession("SESSION-A");

        CompletableFuture<String> secondFuture =
                pushService.registerSession("SESSION-B");

        try {
            // Act
            pushService.onDataReceived(
                    "SESSION-A",
                    "first"
            );

            pushService.onDataReceived(
                    "SESSION-B",
                    "second"
            );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> bothProcessorsStarted.getCount() == 0
                    );

            // Assert while both processors are blocked
            assertEquals(
                    2,
                    maximumConcurrency.get(),
                    "Both session messages should be processed concurrently."
            );

            releaseProcessors.countDown();

            CompletableFuture.allOf(
                    firstFuture,
                    secondFuture
            ).join();

            assertAll(
                    () -> assertEquals(
                            "FIRST",
                            firstFuture.join()
                    ),
                    () -> assertEquals(
                            "SECOND",
                            secondFuture.join()
                    ),
                    () -> assertEquals(
                            2,
                            outboundMessages.size()
                    ),
                    () -> assertEquals(
                            0,
                            pushService.activeSessionCount()
                    )
            );

        } finally {
            releaseProcessors.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that submitting processing after executor shutdown completes the pipeline exceptionally"
    )
    void testMessageProcessingAfterExecutorShutdownIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-014");

        executor.shutdown();

        // Act
        boolean delivered = pushService.onDataReceived(
                "SESSION-014",
                "message"
        );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        RejectedExecutionException.class,
                        rootCause(exception)
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that blank session ID is rejected during registration"
    )
    void testBlankSessionIdIsRejectedDuringRegistration() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.registerSession("   ")
        );

        // Assert
        assertEquals(
                "Session ID is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that null session ID is rejected during registration"
    )
    void testNullSessionIdIsRejectedDuringRegistration() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.registerSession(null)
        );

        // Assert
        assertEquals(
                "Session ID is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that blank payload is rejected before completing the session"
    )
    void testBlankPayloadIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-015");

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.onDataReceived(
                        "SESSION-015",
                        "   "
                )
        );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Raw payload is required.",
                        exception.getMessage()
                ),
                () -> assertFalse(resultFuture.isDone()),
                () -> assertTrue(
                        pushService.hasActiveSession("SESSION-015")
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that null payload is rejected before completing the session"
    )
    void testNullPayloadIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        CompletableFuture<String> resultFuture =
                pushService.registerSession("SESSION-016");

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.onDataReceived(
                        "SESSION-016",
                        null
                )
        );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Raw payload is required.",
                        exception.getMessage()
                ),
                () -> assertFalse(resultFuture.isDone()),
                () -> assertTrue(
                        pushService.hasActiveSession("SESSION-016")
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that timeout registration rejects a null timeout"
    )
    void testNullTimeoutIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> pushService.registerSession(
                        "SESSION-017",
                        null
                )
        );

        // Assert
        assertEquals(
                "timeout must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that timeout registration rejects a zero timeout"
    )
    void testZeroTimeoutIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.registerSession(
                        "SESSION-018",
                        Duration.ZERO
                )
        );

        // Assert
        assertEquals(
                "Timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that timeout registration rejects a negative timeout"
    )
    void testNegativeTimeoutIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> pushService.registerSession(
                        "SESSION-019",
                        Duration.ofMillis(-1)
                )
        );

        // Assert
        assertEquals(
                "Timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that session failure rejects a null failure"
    )
    void testNullSessionFailureIsRejected() {
        // Arrange
        WebSocketPushService pushService = createPushService();

        pushService.registerSession("SESSION-020");

        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> pushService.onSessionFailure(
                        "SESSION-020",
                        null
                )
        );

        // Assert
        assertEquals(
                "failure must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null executor"
    )
    void testConstructorRejectsNullExecutor() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new WebSocketPushService(
                        null,
                        String::toUpperCase,
                        outboundMessages::add,
                        logs::add
                )
        );

        // Assert
        assertEquals(
                "executor must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null message processor"
    )
    void testConstructorRejectsNullMessageProcessor() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new WebSocketPushService(
                        executor,
                        null,
                        outboundMessages::add,
                        logs::add
                )
        );

        // Assert
        assertEquals(
                "messageProcessor must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null outbound sender"
    )
    void testConstructorRejectsNullOutboundSender() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new WebSocketPushService(
                        executor,
                        String::toUpperCase,
                        null,
                        logs::add
                )
        );

        // Assert
        assertEquals(
                "outboundSender must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null logger"
    )
    void testConstructorRejectsNullLogger() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new WebSocketPushService(
                        executor,
                        String::toUpperCase,
                        outboundMessages::add,
                        null
                )
        );

        // Assert
        assertEquals(
                "logger must not be null",
                exception.getMessage()
        );
    }

    private WebSocketPushService createPushService() {
        return new WebSocketPushService(
                executor,
                rawMessage ->
                        "Processed JSON Payload: "
                                + rawMessage.toUpperCase(),
                outboundMessages::add,
                logs::add
        );
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
}