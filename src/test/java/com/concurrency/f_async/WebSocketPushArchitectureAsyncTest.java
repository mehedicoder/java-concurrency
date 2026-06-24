package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class WebSocketPushArchitectureAsyncTest {

    private ExecutorService executor;
    private List<String> outboundMessages;
    private TestSessionRepository sessionRepository;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
        outboundMessages = new CopyOnWriteArrayList<>();
        sessionRepository = new TestSessionRepository();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();

        try {
            assertTrue(
                    executor.awaitTermination(2, TimeUnit.SECONDS),
                    "The test executor did not terminate."
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            fail(
                    "The test thread was interrupted during shutdown.",
                    exception
            );
        }
    }

    @Test
    @DisplayName(
            "Registration authenticates, loads permissions, persists the session and returns a handle"
    )
    void testSessionRegistrationCompletesSuccessfully() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        // Act
        SessionHandle handle = service.registerSessionAsync(
                "SESSION-001",
                "VALID-TOKEN-001"
        ).join();

        // Assert
        PersistedSession persisted =
                sessionRepository.find("SESSION-001");

        assertAll(
                () -> assertEquals(
                        "SESSION-001",
                        handle.sessionId()
                ),
                () -> assertEquals(
                        "USER-001",
                        handle.userId()
                ),
                () -> assertEquals(
                        Set.of(
                                "WEBSOCKET_CONNECT",
                                "CHECKOUT_EVENTS_READ"
                        ),
                        handle.permissions()
                ),
                () -> assertNotNull(handle.messageResult()),
                () -> assertFalse(
                        handle.messageResult().isDone()
                ),
                () -> assertEquals(
                        1,
                        service.activeSessionCount()
                ),
                () -> assertNotNull(persisted),
                () -> assertEquals(
                        "USER-001",
                        persisted.userId()
                ),
                () -> assertEquals(
                        handle.permissions(),
                        persisted.permissions()
                )
        );
    }

    @Test
    @DisplayName(
            "Registration returns immediately while authentication continues asynchronously"
    )
    void testRegistrationIsAsynchronous() {
        // Arrange
        CountDownLatch authenticationStarted =
                new CountDownLatch(1);

        CountDownLatch releaseAuthentication =
                new CountDownLatch(1);

        AtomicReference<String> authenticationThread =
                new AtomicReference<>();

        AuthenticationService authenticationService =
                accessToken -> CompletableFuture.supplyAsync(
                        () -> {
                            authenticationThread.set(
                                    Thread.currentThread().getName()
                            );

                            authenticationStarted.countDown();

                            try {
                                releaseAuthentication.await();
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(exception);
                            }

                            return new AuthenticatedUser("USER-001");
                        },
                        executor
                );

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        sessionRepository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        try {
            // Act
            CompletableFuture<SessionHandle> registrationFuture =
                    service.registerSessionAsync(
                            "SESSION-ASYNC",
                            "VALID-TOKEN"
                    );

            await()
                    .atMost(Duration.ofSeconds(1))
                    .until(
                            () -> authenticationStarted.getCount() == 0
                    );

            // Assert before releasing authentication
            assertAll(
                    () -> assertFalse(
                            registrationFuture.isDone(),
                            "Registration should still be running."
                    ),
                    () -> assertNotEquals(
                            Thread.currentThread().getName(),
                            authenticationThread.get()
                    ),
                    () -> assertEquals(
                            0,
                            service.activeSessionCount(),
                            "Local registration occurs after remote steps complete."
                    )
            );

            releaseAuthentication.countDown();

            SessionHandle handle =
                    registrationFuture.join();

            assertAll(
                    () -> assertEquals(
                            "SESSION-ASYNC",
                            handle.sessionId()
                    ),
                    () -> assertEquals(
                            1,
                            service.activeSessionCount()
                    )
            );

        } finally {
            releaseAuthentication.countDown();
        }
    }

    @Test
    @DisplayName(
            "Registration stages execute in their required sequential order"
    )
    void testRegistrationStagesExecuteSequentially() {
        // Arrange
        List<String> executionOrder =
                new CopyOnWriteArrayList<>();

        AuthenticationService authenticationService =
                accessToken -> {
                    executionOrder.add("authenticate");
                    return CompletableFuture.completedFuture(
                            new AuthenticatedUser("USER-ORDER")
                    );
                };

        PermissionService permissionService =
                userId -> {
                    executionOrder.add("permissions");
                    return CompletableFuture.completedFuture(
                            Set.of("WEBSOCKET_CONNECT")
                    );
                };

        SessionRepository repository =
                new SessionRepository() {
                    @Override
                    public CompletableFuture<Void> saveAsync(
                            PersistedSession session
                    ) {
                        executionOrder.add("persist");
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> deleteAsync(
                            String sessionId
                    ) {
                        executionOrder.add("delete");
                        return CompletableFuture.completedFuture(null);
                    }
                };

        Function<String, String> messageProcessor =
                message -> {
                    executionOrder.add("process-message");
                    return message.toUpperCase();
                };

        Consumer<String> outboundSender =
                message -> executionOrder.add("push-message");

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        permissionService,
                        repository,
                        messageProcessor,
                        outboundSender
                );

        // Act
        SessionHandle handle = service.registerSessionAsync(
                "SESSION-ORDER",
                "VALID-TOKEN"
        ).join();

        service.onDataReceived(
                "SESSION-ORDER",
                "hello"
        );

        handle.messageResult().join();

        // Assert
        assertEquals(
                List.of(
                        "authenticate",
                        "permissions",
                        "persist",
                        "process-message",
                        "push-message"
                ),
                executionOrder
        );
    }

    @Test
    @DisplayName(
            "Multiple users concurrently explicitly register their own sessions and complete their push pipelines"
    )
    void testMultipleUsersRegisterAndCompleteOwnSessionsConcurrently() {
        // Arrange
        record UserSession(
                String accessToken,
                String userId,
                String sessionId,
                String rawMessage,
                String expectedResult
        ) {
        }

        List<UserSession> users = List.of(
                new UserSession(
                        "TOKEN-101",
                        "USER-101",
                        "SESSION-USER-101",
                        "message-from-user-101",
                        "PROCESSED-MESSAGE-FROM-USER-101"
                ),
                new UserSession(
                        "TOKEN-202",
                        "USER-202",
                        "SESSION-USER-202",
                        "message-from-user-202",
                        "PROCESSED-MESSAGE-FROM-USER-202"
                ),
                new UserSession(
                        "TOKEN-303",
                        "USER-303",
                        "SESSION-USER-303",
                        "message-from-user-303",
                        "PROCESSED-MESSAGE-FROM-USER-303"
                )
        );

        int userCount = users.size();

        CountDownLatch allAuthenticationsStarted =
                new CountDownLatch(userCount);

        CountDownLatch releaseAuthentications =
                new CountDownLatch(1);

        CountDownLatch allMessageProcessorsStarted =
                new CountDownLatch(userCount);

        CountDownLatch releaseMessageProcessors =
                new CountDownLatch(1);

        AtomicInteger activeAuthentications =
                new AtomicInteger();

        AtomicInteger maximumAuthenticationConcurrency =
                new AtomicInteger();

        AtomicInteger activeMessageProcessors =
                new AtomicInteger();

        AtomicInteger maximumMessageProcessingConcurrency =
                new AtomicInteger();

        /*
         * Records which user was derived from each token.
         */
        ConcurrentHashMap<String, String> tokenToUser =
                new ConcurrentHashMap<>();

        /*
         * Records which message was processed on which worker thread.
         */
        ConcurrentHashMap<String, String> messageProcessingThreads =
                new ConcurrentHashMap<>();

        AuthenticationService authenticationService =
                accessToken -> CompletableFuture.supplyAsync(
                        () -> {
                            int active =
                                    activeAuthentications.incrementAndGet();

                            maximumAuthenticationConcurrency.accumulateAndGet(
                                    active,
                                    Math::max
                            );

                            allAuthenticationsStarted.countDown();

                            try {
                                releaseAuthentications.await();

                                String numericId =
                                        accessToken.replace(
                                                "TOKEN-",
                                                ""
                                        );

                                String userId =
                                        "USER-" + numericId;

                                tokenToUser.put(
                                        accessToken,
                                        userId
                                );

                                return new AuthenticatedUser(userId);

                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(exception);

                            } finally {
                                activeAuthentications.decrementAndGet();
                            }
                        },
                        executor
                );

        PermissionService permissionService =
                userId -> CompletableFuture.completedFuture(
                        Set.of(
                                "WEBSOCKET_CONNECT",
                                "MESSAGE_READ",
                                "OWNER_" + userId
                        )
                );

        Function<String, String> messageProcessor =
                rawMessage -> {
                    int active =
                            activeMessageProcessors.incrementAndGet();

                    maximumMessageProcessingConcurrency.accumulateAndGet(
                            active,
                            Math::max
                    );

                    messageProcessingThreads.put(
                            rawMessage,
                            Thread.currentThread().getName()
                    );

                    allMessageProcessorsStarted.countDown();

                    try {
                        releaseMessageProcessors.await();

                        return "PROCESSED-"
                                + rawMessage.toUpperCase();

                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(exception);

                    } finally {
                        activeMessageProcessors.decrementAndGet();
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        permissionService,
                        sessionRepository,
                        messageProcessor,
                        outboundMessages::add
                );

        try {
            // Act 1: start all user registrations without waiting
            List<CompletableFuture<SessionHandle>>
                    registrationFutures =
                    users.stream()
                            .map(user ->
                                    service.registerSessionAsync(
                                            user.sessionId(),
                                            user.accessToken()
                                    )
                            )
                            .toList();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> allAuthenticationsStarted.getCount() == 0
                    );

            /*
             * All users are authenticating at the same time.
             */
            assertEquals(
                    userCount,
                    maximumAuthenticationConcurrency.get(),
                    "All user registrations should overlap during authentication."
            );

            releaseAuthentications.countDown();

            CompletableFuture.allOf(
                    registrationFutures.toArray(
                            CompletableFuture[]::new
                    )
            ).join();

            List<SessionHandle> handles =
                    registrationFutures.stream()
                            .map(CompletableFuture::join)
                            .toList();

            /*
             * Index handles by session ID, so we can verify each
             * user/session relationship explicitly.
             */
            ConcurrentHashMap<String, SessionHandle>
                    handlesBySession =
                    new ConcurrentHashMap<>();

            handles.forEach(handle ->
                    handlesBySession.put(
                            handle.sessionId(),
                            handle
                    )
            );

            // Assert user-wise registration
            for (UserSession expected : users) {
                SessionHandle actualHandle =
                        handlesBySession.get(
                                expected.sessionId()
                        );

                PersistedSession persisted =
                        sessionRepository.find(
                                expected.sessionId()
                        );

                assertAll(
                        () -> assertNotNull(
                                actualHandle,
                                "Missing handle for "
                                        + expected.sessionId()
                        ),
                        () -> assertEquals(
                                expected.sessionId(),
                                actualHandle.sessionId()
                        ),
                        () -> assertEquals(
                                expected.userId(),
                                actualHandle.userId()
                        ),
                        () -> assertTrue(
                                actualHandle.permissions().contains(
                                        "OWNER_" + expected.userId()
                                )
                        ),
                        () -> assertEquals(
                                expected.userId(),
                                tokenToUser.get(
                                        expected.accessToken()
                                )
                        ),
                        () -> assertNotNull(
                                persisted,
                                "Session was not persisted for "
                                        + expected.userId()
                        ),
                        () -> assertEquals(
                                expected.sessionId(),
                                persisted.sessionId()
                        ),
                        () -> assertEquals(
                                expected.userId(),
                                persisted.userId()
                        )
                );
            }

            assertEquals(
                    userCount,
                    service.activeSessionCount()
            );

            // Act 2: trigger each user's own push pipeline
            for (UserSession user : users) {
                boolean delivered =
                        service.onDataReceived(
                                user.sessionId(),
                                user.rawMessage()
                        );

                assertTrue(
                        delivered,
                        "Message was not delivered for "
                                + user.userId()
                );
            }

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> allMessageProcessorsStarted.getCount() == 0
                    );

            /*
             * All users' messages are being processed concurrently.
             */
            assertEquals(
                    userCount,
                    maximumMessageProcessingConcurrency.get(),
                    "All user push pipelines should overlap."
            );

            releaseMessageProcessors.countDown();

            CompletableFuture.allOf(
                    handles.stream()
                            .map(SessionHandle::messageResult)
                            .toArray(CompletableFuture[]::new)
            ).join();

            // Assert user-wise push results
            for (UserSession expected : users) {
                SessionHandle handle =
                        handlesBySession.get(
                                expected.sessionId()
                        );

                assertAll(
                        () -> assertEquals(
                                expected.expectedResult(),
                                handle.messageResult().join(),
                                "Wrong push result for "
                                        + expected.userId()
                        ),
                        () -> assertTrue(
                                outboundMessages.contains(
                                        expected.expectedResult()
                                ),
                                "Outbound message missing for "
                                        + expected.userId()
                        ),
                        () -> assertNotNull(
                                messageProcessingThreads.get(
                                        expected.rawMessage()
                                )
                        ),
                        () -> assertNotEquals(
                                Thread.currentThread().getName(),
                                messageProcessingThreads.get(
                                        expected.rawMessage()
                                )
                        )
                );
            }

            assertAll(
                    () -> assertEquals(
                            userCount,
                            outboundMessages.size()
                    ),
                    () -> assertEquals(
                            0,
                            service.activeSessionCount(),
                            "Every one-shot user session should be removed."
                    )
            );

        } finally {
            releaseAuthentications.countDown();
            releaseMessageProcessors.countDown();
        }
    }

    @Test
    @DisplayName(
            "Access token, user ID and persisted session data are propagated correctly"
    )
    void testRegistrationDataIsPropagatedBetweenStages() {
        // Arrange
        AtomicReference<String> receivedToken =
                new AtomicReference<>();

        AtomicReference<String> permissionUserId =
                new AtomicReference<>();

        AtomicReference<PersistedSession> persistedSession =
                new AtomicReference<>();

        AuthenticationService authenticationService =
                accessToken -> {
                    receivedToken.set(accessToken);

                    return CompletableFuture.completedFuture(
                            new AuthenticatedUser("USER-PROPAGATED")
                    );
                };

        PermissionService permissionService =
                userId -> {
                    permissionUserId.set(userId);

                    return CompletableFuture.completedFuture(
                            Set.of("READ", "WRITE")
                    );
                };

        SessionRepository repository =
                new SessionRepository() {
                    @Override
                    public CompletableFuture<Void> saveAsync(
                            PersistedSession session
                    ) {
                        persistedSession.set(session);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> deleteAsync(
                            String sessionId
                    ) {
                        return CompletableFuture.completedFuture(null);
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        permissionService,
                        repository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        // Act
        SessionHandle handle = service.registerSessionAsync(
                "SESSION-PROPAGATED",
                "ACCESS-TOKEN-XYZ"
        ).join();

        // Assert
        assertAll(
                () -> assertEquals(
                        "ACCESS-TOKEN-XYZ",
                        receivedToken.get()
                ),
                () -> assertEquals(
                        "USER-PROPAGATED",
                        permissionUserId.get()
                ),
                () -> assertNotNull(
                        persistedSession.get()
                ),
                () -> assertEquals(
                        "SESSION-PROPAGATED",
                        persistedSession.get().sessionId()
                ),
                () -> assertEquals(
                        "USER-PROPAGATED",
                        persistedSession.get().userId()
                ),
                () -> assertEquals(
                        Set.of("READ", "WRITE"),
                        persistedSession.get().permissions()
                ),
                () -> assertEquals(
                        persistedSession.get().permissions(),
                        handle.permissions()
                )
        );
    }

    @Test
    @DisplayName(
            "An authentication failure prevents permission loading and persistence"
    )
    void testAuthenticationFailureStopsRegistration() {
        // Arrange
        AtomicBoolean permissionServiceCalled =
                new AtomicBoolean(false);

        AtomicBoolean repositoryCalled =
                new AtomicBoolean(false);

        AuthenticationFailureException failure =
                new AuthenticationFailureException(
                        "Invalid WebSocket access token."
                );

        AuthenticationService authenticationService =
                accessToken ->
                        CompletableFuture.failedFuture(failure);

        PermissionService permissionService =
                userId -> {
                    permissionServiceCalled.set(true);
                    return CompletableFuture.completedFuture(Set.of());
                };

        SessionRepository repository =
                new SessionRepository() {
                    @Override
                    public CompletableFuture<Void> saveAsync(
                            PersistedSession session
                    ) {
                        repositoryCalled.set(true);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> deleteAsync(
                            String sessionId
                    ) {
                        return CompletableFuture.completedFuture(null);
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        permissionService,
                        repository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        // Act
        CompletableFuture<SessionHandle> registrationFuture =
                service.registerSessionAsync(
                        "SESSION-AUTH-FAILURE",
                        "INVALID-TOKEN"
                );

        CompletionException exception = assertThrows(
                CompletionException.class,
                registrationFuture::join
        );

        // Assert
        assertAll(
                () -> assertSame(
                        failure,
                        rootCause(exception)
                ),
                () -> assertFalse(
                        permissionServiceCalled.get()
                ),
                () -> assertFalse(
                        repositoryCalled.get()
                ),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "A permission-service failure prevents persistence and local registration"
    )
    void testPermissionFailureStopsRegistration() {
        // Arrange
        AtomicBoolean repositoryCalled =
                new AtomicBoolean(false);

        IllegalStateException failure =
                new IllegalStateException(
                        "Permission service unavailable."
                );

        PermissionService permissionService =
                userId ->
                        CompletableFuture.failedFuture(failure);

        SessionRepository repository =
                new SessionRepository() {
                    @Override
                    public CompletableFuture<Void> saveAsync(
                            PersistedSession session
                    ) {
                        repositoryCalled.set(true);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> deleteAsync(
                            String sessionId
                    ) {
                        return CompletableFuture.completedFuture(null);
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        token -> CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                        permissionService,
                        repository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        // Act
        CompletableFuture<SessionHandle> registrationFuture =
                service.registerSessionAsync(
                        "SESSION-PERMISSION-FAILURE",
                        "VALID-TOKEN"
                );

        CompletionException exception = assertThrows(
                CompletionException.class,
                registrationFuture::join
        );

        // Assert
        assertAll(
                () -> assertSame(
                        failure,
                        rootCause(exception)
                ),
                () -> assertFalse(repositoryCalled.get()),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "A repository save failure prevents local session registration"
    )
    void testRepositoryFailureStopsRegistration() {
        // Arrange
        IllegalStateException failure =
                new IllegalStateException(
                        "Session repository unavailable."
                );

        SessionRepository repository =
                new SessionRepository() {
                    @Override
                    public CompletableFuture<Void> saveAsync(
                            PersistedSession session
                    ) {
                        return CompletableFuture.failedFuture(failure);
                    }

                    @Override
                    public CompletableFuture<Void> deleteAsync(
                            String sessionId
                    ) {
                        return CompletableFuture.completedFuture(null);
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        token -> CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        repository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        // Act
        CompletableFuture<SessionHandle> registrationFuture =
                service.registerSessionAsync(
                        "SESSION-SAVE-FAILURE",
                        "VALID-TOKEN"
                );

        CompletionException exception = assertThrows(
                CompletionException.class,
                registrationFuture::join
        );

        // Assert
        assertAll(
                () -> assertSame(
                        failure,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "An incoming message is processed and pushed after registration"
    )
    void testIncomingMessageIsProcessedAndPushed() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-MESSAGE",
                "VALID-TOKEN"
        ).join();

        // Act
        boolean delivered = service.onDataReceived(
                "SESSION-MESSAGE",
                "{ \"action\": \"checkout\" }"
        );

        String result = handle.messageResult().join();

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertEquals(
                        "Processed JSON Payload: "
                                + "{ \"ACTION\": \"CHECKOUT\" }",
                        result
                ),
                () -> assertEquals(
                        List.of(result),
                        outboundMessages
                ),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Message processing runs on an executor worker thread"
    )
    void testMessageProcessingRunsAsynchronously() {
        // Arrange
        String testThreadName =
                Thread.currentThread().getName();

        AtomicReference<String> processingThread =
                new AtomicReference<>();

        WebSocketRegistrationService service =
                createRegistrationService(
                        token -> CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        sessionRepository,
                        message -> {
                            processingThread.set(
                                    Thread.currentThread().getName()
                            );

                            return message.toUpperCase();
                        },
                        outboundMessages::add
                );

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-THREAD",
                "VALID-TOKEN"
        ).join();

        // Act
        service.onDataReceived(
                "SESSION-THREAD",
                "hello"
        );

        String result = handle.messageResult().join();

        // Assert
        assertAll(
                () -> assertEquals("HELLO", result),
                () -> assertNotNull(processingThread.get()),
                () -> assertNotEquals(
                        testThreadName,
                        processingThread.get()
                )
        );
    }

    @Test
    @DisplayName(
            "Only the first message is delivered for a one-shot session"
    )
    void testOnlyFirstMessageIsDelivered() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-ONE-SHOT",
                "VALID-TOKEN"
        ).join();

        // Act
        boolean firstDelivery = service.onDataReceived(
                "SESSION-ONE-SHOT",
                "first"
        );

        boolean secondDelivery = service.onDataReceived(
                "SESSION-ONE-SHOT",
                "second"
        );

        String result = handle.messageResult().join();

        // Assert
        assertAll(
                () -> assertTrue(firstDelivery),
                () -> assertFalse(secondDelivery),
                () -> assertEquals(
                        "Processed JSON Payload: FIRST",
                        result
                ),
                () -> assertEquals(
                        1,
                        outboundMessages.size()
                )
        );
    }

    @Test
    @DisplayName(
            "A message for an unknown session returns false"
    )
    void testMessageForUnknownSessionReturnsFalse() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        // Act
        boolean delivered = service.onDataReceived(
                "UNKNOWN-SESSION",
                "message"
        );

        // Assert
        assertAll(
                () -> assertFalse(delivered),
                () -> assertTrue(outboundMessages.isEmpty())
        );
    }

    @Test
    @DisplayName(
            "A message processor failure completes the message future exceptionally"
    )
    void testMessageProcessorFailure() {
        // Arrange
        IllegalStateException failure =
                new IllegalStateException(
                        "Invalid WebSocket payload."
                );

        WebSocketRegistrationService service =
                createRegistrationService(
                        token -> CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        sessionRepository,
                        message -> {
                            throw failure;
                        },
                        outboundMessages::add
                );

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-PROCESSOR-FAILURE",
                "VALID-TOKEN"
        ).join();

        // Act
        boolean delivered = service.onDataReceived(
                "SESSION-PROCESSOR-FAILURE",
                "bad-message"
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                handle.messageResult()::join
        );

        // Assert
        assertAll(
                () -> assertTrue(delivered),
                () -> assertSame(
                        failure,
                        rootCause(exception)
                ),
                () -> assertTrue(outboundMessages.isEmpty()),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "An outbound sender failure completes the message future exceptionally"
    )
    void testOutboundSenderFailure() {
        // Arrange
        IllegalStateException failure =
                new IllegalStateException(
                        "WebSocket connection closed."
                );

        WebSocketRegistrationService service =
                createRegistrationService(
                        token -> CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        sessionRepository,
                        String::toUpperCase,
                        message -> {
                            throw failure;
                        }
                );

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-SENDER-FAILURE",
                "VALID-TOKEN"
        ).join();

        // Act
        service.onDataReceived(
                "SESSION-SENDER-FAILURE",
                "hello"
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                handle.messageResult()::join
        );

        // Assert
        assertAll(
                () -> assertSame(
                        failure,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Duplicate local registration completes the second registration exceptionally"
    )
    void testDuplicateSessionRegistrationIsRejected() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        SessionHandle firstHandle =
                service.registerSessionAsync(
                        "SESSION-DUPLICATE",
                        "VALID-TOKEN-1"
                ).join();

        // Act
        CompletableFuture<SessionHandle> secondRegistration =
                service.registerSessionAsync(
                        "SESSION-DUPLICATE",
                        "VALID-TOKEN-2"
                );

        CompletionException exception = assertThrows(
                CompletionException.class,
                secondRegistration::join
        );

        // Assert
        assertAll(
                () -> assertInstanceOf(
                        DuplicateSessionException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        "Session is already registered: SESSION-DUPLICATE",
                        rootCause(exception).getMessage()
                ),
                () -> assertEquals(
                        1,
                        service.activeSessionCount()
                ),
                () -> assertFalse(
                        firstHandle.messageResult().isDone()
                )
        );
    }

    @Test
    @DisplayName(
            "Disconnecting an active session cancels the message pipeline and deletes persistence"
    )
    void testDisconnectActiveSession() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-DISCONNECT",
                "VALID-TOKEN"
        ).join();

        assertNotNull(
                sessionRepository.find("SESSION-DISCONNECT")
        );

        // Act
        boolean disconnected = service.disconnectSessionAsync(
                "SESSION-DISCONNECT"
        ).join();


        // Assert
        assertAll(
                () -> assertTrue(disconnected),
                () -> assertTrue(
                        handle.messageResult().isCancelled()
                ),
                () -> assertThrows(
                        CancellationException.class,
                        handle.messageResult()::join
                ),
                () -> assertNull(
                        sessionRepository.find("SESSION-DISCONNECT")
                ),
                () -> assertEquals(
                        0,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Disconnecting an unknown session returns false"
    )
    void testDisconnectUnknownSession() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        // Act
        boolean disconnected = service.disconnectSessionAsync(
                "UNKNOWN-SESSION"
        ).join();

        // Assert
        assertAll(
                () -> assertFalse(disconnected),
                () -> assertTrue(
                        sessionRepository.deletedSessionIds().isEmpty()
                )
        );
    }

    @Test
    @DisplayName(
            "Multiple registration operations can overlap"
    )
    void testMultipleRegistrationsCanRunConcurrently() {
        // Arrange
        CountDownLatch bothAuthenticationsStarted =
                new CountDownLatch(2);

        CountDownLatch releaseAuthentications =
                new CountDownLatch(1);

        AtomicInteger activeAuthentications =
                new AtomicInteger();

        AtomicInteger maximumConcurrency =
                new AtomicInteger();

        AuthenticationService authenticationService =
                accessToken -> CompletableFuture.supplyAsync(
                        () -> {
                            int active =
                                    activeAuthentications.incrementAndGet();

                            maximumConcurrency.accumulateAndGet(
                                    active,
                                    Math::max
                            );

                            bothAuthenticationsStarted.countDown();

                            try {
                                releaseAuthentications.await();
                                return new AuthenticatedUser(
                                        "USER-" + accessToken
                                );
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(exception);
                            } finally {
                                activeAuthentications.decrementAndGet();
                            }
                        },
                        executor
                );

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        userId -> CompletableFuture.completedFuture(
                                Set.of("WEBSOCKET_CONNECT")
                        ),
                        sessionRepository,
                        String::toUpperCase,
                        outboundMessages::add
                );

        try {
            // Act
            CompletableFuture<SessionHandle> first =
                    service.registerSessionAsync(
                            "SESSION-A",
                            "TOKEN-A"
                    );

            CompletableFuture<SessionHandle> second =
                    service.registerSessionAsync(
                            "SESSION-B",
                            "TOKEN-B"
                    );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> bothAuthenticationsStarted.getCount() == 0
                    );

            // Assert while both are blocked
            assertEquals(
                    2,
                    maximumConcurrency.get(),
                    "Both registrations should overlap."
            );

            releaseAuthentications.countDown();

            CompletableFuture.allOf(first, second).join();

            assertAll(
                    () -> assertEquals(
                            2,
                            service.activeSessionCount()
                    ),
                    () -> assertEquals(
                            "SESSION-A",
                            first.join().sessionId()
                    ),
                    () -> assertEquals(
                            "SESSION-B",
                            second.join().sessionId()
                    )
            );

        } finally {
            releaseAuthentications.countDown();
        }
    }

    @Test
    @DisplayName(
            "Multiple sessions start and complete their push pipelines concurrently end-to-end"
    )
    void testMultipleUsersCompleteFullPipelineConcurrently() {
        // Arrange
        int numberOfUsers = 3;

        CountDownLatch allAuthenticationsStarted =
                new CountDownLatch(numberOfUsers);

        CountDownLatch releaseAuthentications =
                new CountDownLatch(1);

        CountDownLatch allMessageProcessorsStarted =
                new CountDownLatch(numberOfUsers);

        CountDownLatch releaseMessageProcessors =
                new CountDownLatch(1);

        AtomicInteger activeAuthentications =
                new AtomicInteger();

        AtomicInteger maximumAuthenticationConcurrency =
                new AtomicInteger();

        AtomicInteger activeMessageProcessors =
                new AtomicInteger();

        AtomicInteger maximumMessageProcessingConcurrency =
                new AtomicInteger();

        List<String> processingThreads =
                new CopyOnWriteArrayList<>();

        AuthenticationService authenticationService =
                accessToken -> CompletableFuture.supplyAsync(
                        () -> {
                            int active =
                                    activeAuthentications.incrementAndGet();

                            maximumAuthenticationConcurrency.accumulateAndGet(
                                    active,
                                    Math::max
                            );

                            allAuthenticationsStarted.countDown();

                            try {
                                releaseAuthentications.await();

                                String userNumber =
                                        accessToken.replace(
                                                "TOKEN-",
                                                ""
                                        );

                                return new AuthenticatedUser(
                                        "USER-" + userNumber
                                );

                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(exception);

                            } finally {
                                activeAuthentications.decrementAndGet();
                            }
                        },
                        executor
                );

        PermissionService permissionService =
                userId -> CompletableFuture.completedFuture(
                        Set.of(
                                "WEBSOCKET_CONNECT",
                                "MESSAGE_READ",
                                "USER_" + userId
                        )
                );

        Function<String, String> messageProcessor =
                rawMessage -> {
                    int active =
                            activeMessageProcessors.incrementAndGet();

                    maximumMessageProcessingConcurrency.accumulateAndGet(
                            active,
                            Math::max
                    );

                    processingThreads.add(
                            Thread.currentThread().getName()
                    );

                    allMessageProcessorsStarted.countDown();

                    try {
                        releaseMessageProcessors.await();

                        return "PROCESSED-" + rawMessage.toUpperCase();

                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(exception);

                    } finally {
                        activeMessageProcessors.decrementAndGet();
                    }
                };

        WebSocketRegistrationService service =
                createRegistrationService(
                        authenticationService,
                        permissionService,
                        sessionRepository,
                        messageProcessor,
                        outboundMessages::add
                );

        CompletableFuture<SessionHandle> firstRegistration = null;
        CompletableFuture<SessionHandle> secondRegistration = null;
        CompletableFuture<SessionHandle> thirdRegistration = null;

        try {
            // Act: start three registration pipelines
            firstRegistration =
                    service.registerSessionAsync(
                            "SESSION-1",
                            "TOKEN-1"
                    );

            secondRegistration =
                    service.registerSessionAsync(
                            "SESSION-2",
                            "TOKEN-2"
                    );

            thirdRegistration =
                    service.registerSessionAsync(
                            "SESSION-3",
                            "TOKEN-3"
                    );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> allAuthenticationsStarted.getCount() == 0
                    );

            /*
             * All three authentication operations are currently blocked,
             * proving that registration pipelines overlap.
             */
            assertEquals(
                    numberOfUsers,
                    maximumAuthenticationConcurrency.get(),
                    "All user authentication operations should overlap."
            );

            releaseAuthentications.countDown();

            CompletableFuture.allOf(
                    firstRegistration,
                    secondRegistration,
                    thirdRegistration
            ).join();

            SessionHandle firstHandle =
                    firstRegistration.join();

            SessionHandle secondHandle =
                    secondRegistration.join();

            SessionHandle thirdHandle =
                    thirdRegistration.join();

            // Verify all sessions completed registration
            assertAll(
                    () -> assertEquals(
                            "USER-1",
                            firstHandle.userId()
                    ),
                    () -> assertEquals(
                            "USER-2",
                            secondHandle.userId()
                    ),
                    () -> assertEquals(
                            "USER-3",
                            thirdHandle.userId()
                    ),
                    () -> assertNotNull(
                            sessionRepository.find("SESSION-1")
                    ),
                    () -> assertNotNull(
                            sessionRepository.find("SESSION-2")
                    ),
                    () -> assertNotNull(
                            sessionRepository.find("SESSION-3")
                    ),
                    () -> assertEquals(
                            numberOfUsers,
                            service.activeSessionCount()
                    )
            );

            // Trigger all three push pipelines
            boolean firstDelivered =
                    service.onDataReceived(
                            "SESSION-1",
                            "message-from-user-1"
                    );

            boolean secondDelivered =
                    service.onDataReceived(
                            "SESSION-2",
                            "message-from-user-2"
                    );

            boolean thirdDelivered =
                    service.onDataReceived(
                            "SESSION-3",
                            "message-from-user-3"
                    );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(
                            () -> allMessageProcessorsStarted.getCount() == 0
                    );

            /*
             * All message processors are blocked at the same time,
             * proving concurrent push processing.
             */
            assertEquals(
                    numberOfUsers,
                    maximumMessageProcessingConcurrency.get(),
                    "All message-processing pipelines should overlap."
            );

            releaseMessageProcessors.countDown();

            CompletableFuture.allOf(
                    firstHandle.messageResult(),
                    secondHandle.messageResult(),
                    thirdHandle.messageResult()
            ).join();

            // Assert the complete multi-user lifecycle
            assertAll(
                    () -> assertTrue(firstDelivered),
                    () -> assertTrue(secondDelivered),
                    () -> assertTrue(thirdDelivered),

                    () -> assertEquals(
                            "PROCESSED-MESSAGE-FROM-USER-1",
                            firstHandle.messageResult().join()
                    ),
                    () -> assertEquals(
                            "PROCESSED-MESSAGE-FROM-USER-2",
                            secondHandle.messageResult().join()
                    ),
                    () -> assertEquals(
                            "PROCESSED-MESSAGE-FROM-USER-3",
                            thirdHandle.messageResult().join()
                    ),

                    () -> assertEquals(
                            numberOfUsers,
                            outboundMessages.size()
                    ),
                    () -> assertTrue(
                            outboundMessages.contains(
                                    "PROCESSED-MESSAGE-FROM-USER-1"
                            )
                    ),
                    () -> assertTrue(
                            outboundMessages.contains(
                                    "PROCESSED-MESSAGE-FROM-USER-2"
                            )
                    ),
                    () -> assertTrue(
                            outboundMessages.contains(
                                    "PROCESSED-MESSAGE-FROM-USER-3"
                            )
                    ),

                    () -> assertEquals(
                            numberOfUsers,
                            processingThreads.size()
                    ),
                    () -> assertTrue(
                            processingThreads.stream()
                                    .noneMatch(
                                            threadName ->
                                                    threadName.equals(
                                                            Thread.currentThread()
                                                                    .getName()
                                                    )
                                    ),
                            "Message processing should not execute on the test thread."
                    ),

                    () -> assertEquals(
                            0,
                            service.activeSessionCount(),
                            "All one-shot sessions should be removed after delivery."
                    )
            );

        } finally {
            releaseAuthentications.countDown();
            releaseMessageProcessors.countDown();
        }
    }

    @Test
    @DisplayName(
            "Null and blank session IDs are rejected synchronously"
    )
    void testInvalidSessionIdIsRejected() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        // Act
        IllegalArgumentException nullException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.registerSessionAsync(
                                null,
                                "VALID-TOKEN"
                        )
                );

        IllegalArgumentException blankException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.registerSessionAsync(
                                "   ",
                                "VALID-TOKEN"
                        )
                );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Session ID is required.",
                        nullException.getMessage()
                ),
                () -> assertEquals(
                        "Session ID is required.",
                        blankException.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Null and blank access tokens are rejected synchronously"
    )
    void testInvalidAccessTokenIsRejected() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        // Act
        IllegalArgumentException nullException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.registerSessionAsync(
                                "SESSION-001",
                                null
                        )
                );

        IllegalArgumentException blankException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.registerSessionAsync(
                                "SESSION-002",
                                "   "
                        )
                );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Access token is required.",
                        nullException.getMessage()
                ),
                () -> assertEquals(
                        "Access token is required.",
                        blankException.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Null and blank payloads are rejected without removing the active session"
    )
    void testInvalidPayloadIsRejected() {
        // Arrange
        WebSocketRegistrationService service =
                createRegistrationService();

        SessionHandle handle = service.registerSessionAsync(
                "SESSION-PAYLOAD",
                "VALID-TOKEN"
        ).join();

        // Act
        IllegalArgumentException nullException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.onDataReceived(
                                "SESSION-PAYLOAD",
                                null
                        )
                );

        IllegalArgumentException blankException =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.onDataReceived(
                                "SESSION-PAYLOAD",
                                "   "
                        )
                );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Raw payload is required.",
                        nullException.getMessage()
                ),
                () -> assertEquals(
                        "Raw payload is required.",
                        blankException.getMessage()
                ),
                () -> assertFalse(
                        handle.messageResult().isDone()
                ),
                () -> assertEquals(
                        1,
                        service.activeSessionCount()
                )
        );
    }

    @Test
    @DisplayName(
            "Constructor rejects null dependencies"
    )
    void testConstructorRejectsNullDependencies() {
        AuthenticationService authenticationService =
                token -> CompletableFuture.completedFuture(
                        new AuthenticatedUser("USER-001")
                );

        PermissionService permissionService =
                userId -> CompletableFuture.completedFuture(
                        Set.of("WEBSOCKET_CONNECT")
                );

        Function<String, String> processor =
                String::toUpperCase;

        Consumer<String> sender =
                outboundMessages::add;

        assertAll(
                () -> assertEquals(
                        "executor must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        null,
                                        authenticationService,
                                        permissionService,
                                        sessionRepository,
                                        processor,
                                        sender
                                )
                        ).getMessage()
                ),
                () -> assertEquals(
                        "authenticationService must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        executor,
                                        null,
                                        permissionService,
                                        sessionRepository,
                                        processor,
                                        sender
                                )
                        ).getMessage()
                ),
                () -> assertEquals(
                        "permissionService must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        executor,
                                        authenticationService,
                                        null,
                                        sessionRepository,
                                        processor,
                                        sender
                                )
                        ).getMessage()
                ),
                () -> assertEquals(
                        "sessionRepository must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        executor,
                                        authenticationService,
                                        permissionService,
                                        null,
                                        processor,
                                        sender
                                )
                        ).getMessage()
                ),
                () -> assertEquals(
                        "messageProcessor must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        executor,
                                        authenticationService,
                                        permissionService,
                                        sessionRepository,
                                        null,
                                        sender
                                )
                        ).getMessage()
                ),
                () -> assertEquals(
                        "outboundSender must not be null",
                        assertThrows(
                                NullPointerException.class,
                                () -> new WebSocketRegistrationService(
                                        executor,
                                        authenticationService,
                                        permissionService,
                                        sessionRepository,
                                        processor,
                                        null
                                )
                        ).getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "SessionHandle creates an immutable copy of permissions"
    )
    void testSessionHandleDefensivelyCopiesPermissions() {
        // Arrange
        Set<String> mutablePermissions =
                ConcurrentHashMap.newKeySet();

        mutablePermissions.add("READ");

        CompletableFuture<String> messageResult =
                new CompletableFuture<>();

        // Act
        SessionHandle handle = new SessionHandle(
                "SESSION-IMMUTABLE",
                "USER-IMMUTABLE",
                mutablePermissions,
                messageResult
        );

        mutablePermissions.add("WRITE");

        // Assert
        assertAll(
                () -> assertEquals(
                        Set.of("READ"),
                        handle.permissions()
                ),
                () -> assertThrows(
                        UnsupportedOperationException.class,
                        () -> handle.permissions().add("ADMIN")
                )
        );
    }

    private WebSocketRegistrationService
    createRegistrationService() {
        return createRegistrationService(
                accessToken ->
                        CompletableFuture.completedFuture(
                                new AuthenticatedUser("USER-001")
                        ),
                userId ->
                        CompletableFuture.completedFuture(
                                Set.of(
                                        "WEBSOCKET_CONNECT",
                                        "CHECKOUT_EVENTS_READ"
                                )
                        ),
                sessionRepository,
                rawMessage ->
                        "Processed JSON Payload: "
                                + rawMessage.toUpperCase(),
                outboundMessages::add
        );
    }

    private WebSocketRegistrationService
    createRegistrationService(
            AuthenticationService authenticationService,
            PermissionService permissionService,
            SessionRepository repository,
            Function<String, String> messageProcessor,
            Consumer<String> outboundSender
    ) {
        return new WebSocketRegistrationService(
                executor,
                authenticationService,
                permissionService,
                repository,
                messageProcessor,
                outboundSender
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

    private static final class TestSessionRepository
            implements SessionRepository {

        private final ConcurrentHashMap<String, PersistedSession>
                sessions = new ConcurrentHashMap<>();

        private final List<String> deletedSessionIds =
                new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> saveAsync(
                PersistedSession session
        ) {
            sessions.put(session.sessionId(), session);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteAsync(
                String sessionId
        ) {
            sessions.remove(sessionId);
            deletedSessionIds.add(sessionId);

            return CompletableFuture.completedFuture(null);
        }

        PersistedSession find(String sessionId) {
            return sessions.get(sessionId);
        }

        List<String> deletedSessionIds() {
            return new ArrayList<>(deletedSessionIds);
        }
    }
}
