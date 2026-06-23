package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class NonBlockingGatewayAsyncTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();

        try {
            assertTrue(
                    executor.awaitTermination(2, TimeUnit.SECONDS),
                    "The gateway executor failed to terminate."
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
            "Verify successful gateway processing from authentication through response creation"
    )
    void testGatewayRequestCompletesSuccessfully() {
        // Arrange
        SecurityTokenClient securityClient =
                route -> "AUTH_TOKEN_XYZ_123";

        UserDataClient userDataClient =
                token -> "UserMetadata[Mehedi, GoldTier]";

        GatewayService gateway = createGateway(
                securityClient,
                userDataClient,
                Duration.ofSeconds(2)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/user/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        GatewayResponse response = responseFuture.join();

        // Assert
        assertAll(
                () -> assertFalse(
                        responseFuture.isCompletedExceptionally()
                ),
                () -> assertFalse(responseFuture.isCancelled()),
                () -> assertEquals(200, response.statusCode()),
                () -> assertTrue(
                        response.body().contains(
                                "\"status\": \"SUCCESS\""
                        )
                ),
                () -> assertTrue(
                        response.body().contains(
                                "UserMetadata[Mehedi, GoldTier]"
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that the route is passed to the security-token client"
    )
    void testRouteIsPassedToSecurityClient() {
        // Arrange
        AtomicReference<String> receivedRoute =
                new AtomicReference<>();

        SecurityTokenClient securityClient =
                route -> {
                    receivedRoute.set(route);
                    return "AUTH-TOKEN";
                };

        UserDataClient userDataClient =
                token -> "USER-DATA";

        GatewayService gateway = createGateway(
                securityClient,
                userDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        gateway.processIncomingHttpRequest(
                "/api/v1/account"
        ).join();

        // Assert
        assertEquals(
                "/api/v1/account",
                receivedRoute.get()
        );
    }

    @Test
    @DisplayName(
            "Verify that the authentication token is passed to the user-data client"
    )
    void testAuthenticationTokenIsPassedToUserDataClient() {
        // Arrange
        AtomicReference<String> receivedToken =
                new AtomicReference<>();

        SecurityTokenClient securityClient =
                route -> "AUTH-98765";

        UserDataClient userDataClient =
                token -> {
                    receivedToken.set(token);
                    return "PROFILE-DATA";
                };

        GatewayService gateway = createGateway(
                securityClient,
                userDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        GatewayResponse response =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                ).join();

        // Assert
        assertAll(
                () -> assertEquals(
                        "AUTH-98765",
                        receivedToken.get()
                ),
                () -> assertTrue(
                        response.body().contains("PROFILE-DATA")
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that user-data retrieval starts only after authentication succeeds"
    )
    void testUserDataRetrievalRunsAfterAuthentication() {
        // Arrange
        CountDownLatch releaseAuthentication =
                new CountDownLatch(1);

        AtomicBoolean userClientCalled =
                new AtomicBoolean(false);

        SecurityTokenClient delayedSecurityClient =
                route -> {
                    try {
                        releaseAuthentication.await();
                        return "AUTH-TOKEN";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        UserDataClient userDataClient =
                token -> {
                    userClientCalled.set(true);
                    return "USER-DATA";
                };

        GatewayService gateway = createGateway(
                delayedSecurityClient,
                userDataClient,
                Duration.ofSeconds(2)
        );

        try {
            // Act
            CompletableFuture<GatewayResponse> responseFuture =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/profile"
                    );

            await()
                    .during(Duration.ofMillis(100))
                    .atMost(Duration.ofMillis(300))
                    .until(() -> !userClientCalled.get());

            assertFalse(responseFuture.isDone());

            releaseAuthentication.countDown();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(responseFuture::isDone);

            // Assert
            assertAll(
                    () -> assertTrue(userClientCalled.get()),
                    () -> assertEquals(
                            200,
                            responseFuture.join().statusCode()
                    )
            );

        } finally {
            releaseAuthentication.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that a security-client failure prevents the user-data client from running"
    )
    void testSecurityFailureShortCircuitsPipeline() {
        // Arrange
        AtomicBoolean userClientCalled =
                new AtomicBoolean(false);

        SecurityTokenClient brokenSecurityClient =
                route -> {
                    throw new IllegalStateException(
                            "Authentication service unavailable."
                    );
                };

        UserDataClient userDataClient =
                token -> {
                    userClientCalled.set(true);
                    return "SHOULD-NOT-RUN";
                };

        GatewayService gateway = createGateway(
                brokenSecurityClient,
                userDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        Throwable cause = rootCause(exception);

        // Assert
        assertAll(
                () -> assertTrue(
                        responseFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "Authentication service unavailable.",
                        cause.getMessage()
                ),
                () -> assertFalse(
                        userClientCalled.get(),
                        "The dependent user-data stage must not run."
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a null authentication token completes the pipeline exceptionally"
    )
    void testNullAuthenticationTokenCompletesExceptionally() {
        // Arrange
        SecurityTokenClient securityClient =
                route -> null;

        UserDataClient userDataClient =
                token -> "SHOULD-NOT-RUN";

        GatewayService gateway = createGateway(
                securityClient,
                userDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(
                        responseFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        AuthenticationException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        "Authentication service returned an invalid token.",
                        rootCause(exception).getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a blank authentication token completes the pipeline exceptionally"
    )
    void testBlankAuthenticationTokenCompletesExceptionally() {
        // Arrange
        SecurityTokenClient securityClient =
                route -> "   ";

        UserDataClient userDataClient =
                token -> "SHOULD-NOT-RUN";

        GatewayService gateway = createGateway(
                securityClient,
                userDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        // Assert
        assertInstanceOf(
                AuthenticationException.class,
                rootCause(exception)
        );

        assertEquals(
                "Authentication service returned an invalid token.",
                rootCause(exception).getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that a user-data service failure completes the pipeline exceptionally"
    )
    void testUserDataServiceFailureCompletesExceptionally() {
        // Arrange
        SecurityTokenClient securityClient =
                route -> "AUTH-TOKEN";

        UserDataClient brokenUserDataClient =
                token -> {
                    throw new IllegalStateException(
                            "User profile service unavailable."
                    );
                };

        GatewayService gateway = createGateway(
                securityClient,
                brokenUserDataClient,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        Throwable cause = rootCause(exception);

        // Assert
        assertAll(
                () -> assertTrue(
                        responseFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "User profile service unavailable.",
                        cause.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that null user data completes the pipeline exceptionally"
    )
    void testNullUserDataCompletesExceptionally() {
        // Arrange
        GatewayService gateway = createGateway(
                route -> "AUTH-TOKEN",
                token -> null,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        // Assert
        assertAll(
                () -> assertInstanceOf(
                        DownstreamServiceException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        "User service returned an empty response.",
                        rootCause(exception).getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that blank user data completes the pipeline exceptionally"
    )
    void testBlankUserDataCompletesExceptionally() {
        // Arrange
        GatewayService gateway = createGateway(
                route -> "AUTH-TOKEN",
                token -> "   ",
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<GatewayResponse> responseFuture =
                gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(responseFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                responseFuture::join
        );

        // Assert
        assertInstanceOf(
                DownstreamServiceException.class,
                rootCause(exception)
        );

        assertEquals(
                "User service returned an empty response.",
                rootCause(exception).getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that a request exceeding its deadline completes with TimeoutException"
    )
    void testGatewayRequestTimesOut() {
        // Arrange
        CountDownLatch releaseSecurityClient =
                new CountDownLatch(1);

        SecurityTokenClient slowSecurityClient =
                route -> {
                    try {
                        releaseSecurityClient.await();
                        return "LATE-TOKEN";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        GatewayService gateway = createGateway(
                slowSecurityClient,
                token -> "USER-DATA",
                Duration.ofMillis(100)
        );

        try {
            // Act
            CompletableFuture<GatewayResponse> responseFuture =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/profile"
                    );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(responseFuture::isDone);

            CompletionException exception = assertThrows(
                    CompletionException.class,
                    responseFuture::join
            );

            // Assert
            assertAll(
                    () -> assertTrue(
                            responseFuture.isCompletedExceptionally()
                    ),
                    () -> assertInstanceOf(
                            TimeoutException.class,
                            rootCause(exception)
                    )
            );

        } finally {
            releaseSecurityClient.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that a blank route is rejected before asynchronous execution starts"
    )
    void testBlankRouteIsRejectedImmediately() {
        // Arrange
        AtomicBoolean securityClientCalled =
                new AtomicBoolean(false);

        GatewayService gateway = createGateway(
                route -> {
                    securityClientCalled.set(true);
                    return "AUTH-TOKEN";
                },
                token -> "USER-DATA",
                Duration.ofSeconds(1)
        );

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.processIncomingHttpRequest("   ")
        );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Gateway route is required.",
                        exception.getMessage()
                ),
                () -> assertFalse(securityClientCalled.get())
        );
    }

    @Test
    @DisplayName(
            "Verify that a null route is rejected before asynchronous execution starts"
    )
    void testNullRouteIsRejectedImmediately() {
        // Arrange
        GatewayService gateway = createGateway(
                route -> "AUTH-TOKEN",
                token -> "USER-DATA",
                Duration.ofSeconds(1)
        );

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.processIncomingHttpRequest(null)
        );

        // Assert
        assertEquals(
                "Gateway route is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that submitting a request after executor shutdown is rejected"
    )
    void testSubmissionAfterExecutorShutdownIsRejected() {
        // Arrange
        GatewayService gateway = createGateway(
                route -> "AUTH-TOKEN",
                token -> "USER-DATA",
                Duration.ofSeconds(1)
        );

        executor.shutdown();

        // Act & Assert
        assertThrows(
                RejectedExecutionException.class,
                () -> gateway.processIncomingHttpRequest(
                        "/api/v1/profile"
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that multiple gateway requests can execute concurrently"
    )
    void testMultipleGatewayRequestsExecuteConcurrently() {
        // Arrange
        CountDownLatch bothSecurityCallsStarted =
                new CountDownLatch(2);

        CountDownLatch releaseSecurityCalls =
                new CountDownLatch(1);

        SecurityTokenClient blockingSecurityClient =
                route -> {
                    bothSecurityCallsStarted.countDown();

                    try {
                        releaseSecurityCalls.await();
                        return "TOKEN-" + route;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        UserDataClient userDataClient =
                token -> "DATA-" + token;

        GatewayService gateway = createGateway(
                blockingSecurityClient,
                userDataClient,
                Duration.ofSeconds(2)
        );

        try {
            // Act
            CompletableFuture<GatewayResponse> first =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/user/1"
                    );

            CompletableFuture<GatewayResponse> second =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/user/2"
                    );

            await()
                    .atMost(Duration.ofSeconds(1))
                    .until(
                            () -> bothSecurityCallsStarted.getCount() == 0
                    );

            assertFalse(first.isDone());
            assertFalse(second.isDone());

            releaseSecurityCalls.countDown();

            CompletableFuture<Void> allRequests =
                    CompletableFuture.allOf(first, second);

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(allRequests::isDone);

            // Assert
            assertAll(
                    () -> assertEquals(
                            200,
                            first.join().statusCode()
                    ),
                    () -> assertEquals(
                            200,
                            second.join().statusCode()
                    ),
                    () -> assertTrue(
                            first.join().body().contains(
                                    "TOKEN-/api/v1/user/1"
                            )
                    ),
                    () -> assertTrue(
                            second.join().body().contains(
                                    "TOKEN-/api/v1/user/2"
                            )
                    )
            );

        } finally {
            releaseSecurityCalls.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that GatewayService rejects a null executor"
    )
    void testGatewayRejectsNullExecutor() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        null,
                        route -> "TOKEN",
                        token -> "DATA",
                        Duration.ofSeconds(1)
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
            "Verify that GatewayService rejects a null security client"
    )
    void testGatewayRejectsNullSecurityClient() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        null,
                        token -> "DATA",
                        Duration.ofSeconds(1)
                )
        );

        // Assert
        assertEquals(
                "securityClient must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that GatewayService rejects a null user-data client"
    )
    void testGatewayRejectsNullUserDataClient() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        route -> "TOKEN",
                        null,
                        Duration.ofSeconds(1)
                )
        );

        // Assert
        assertEquals(
                "userDataClient must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that GatewayService rejects a null request timeout"
    )
    void testGatewayRejectsNullTimeout() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        route -> "TOKEN",
                        token -> "DATA",
                        null
                )
        );

        // Assert
        assertEquals(
                "requestTimeout must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that GatewayService rejects a zero request timeout"
    )
    void testGatewayRejectsZeroTimeout() {
        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        route -> "TOKEN",
                        token -> "DATA",
                        Duration.ZERO
                )
        );

        // Assert
        assertEquals(
                "Request timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that GatewayService rejects a negative request timeout"
    )
    void testGatewayRejectsNegativeTimeout() {
        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        route -> "TOKEN",
                        token -> "DATA",
                        Duration.ofMillis(-1)
                )
        );

        // Assert
        assertEquals(
                "Request timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that GatewayResponse rejects a null response body"
    )
    void testGatewayResponseRejectsNullBody() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayResponse(200, null)
        );

        // Assert
        assertEquals(
                "body must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that shutdown between dependent stages can reject the next asynchronous stage"
    )
    void testShutdownBeforeDependentStageSubmissionRejectsPipeline() {
        // Arrange
        CountDownLatch authenticationStarted =
                new CountDownLatch(1);

        CountDownLatch releaseAuthentication =
                new CountDownLatch(1);

        SecurityTokenClient delayedSecurityClient =
                route -> {
                    authenticationStarted.countDown();

                    try {
                        releaseAuthentication.await();
                        return "AUTH-TOKEN";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        GatewayService gateway = createGateway(
                delayedSecurityClient,
                token -> "USER-DATA",
                Duration.ofSeconds(2)
        );

        try {
            // Act
            CompletableFuture<GatewayResponse> responseFuture =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/profile"
                    );

            await()
                    .atMost(Duration.ofSeconds(1))
                    .until(() -> authenticationStarted.getCount() == 0);

            executor.shutdown();

            releaseAuthentication.countDown();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(responseFuture::isDone);

            CompletionException exception = assertThrows(
                    CompletionException.class,
                    responseFuture::join
            );

            // Assert
            assertAll(
                    () -> assertTrue(
                            responseFuture.isCompletedExceptionally()
                    ),
                    () -> assertInstanceOf(
                            RejectedExecutionException.class,
                            rootCause(exception)
                    )
            );

        } finally {
            releaseAuthentication.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that graceful shutdown allows an already-submitted executor task to finish"
    )
    void testGracefulShutdownAllowsSubmittedTaskToFinish()
            throws Exception {

        // Arrange
        CompletableFuture<String> task =
                CompletableFuture.supplyAsync(
                        () -> "COMPLETED",
                        executor
                );

        // Act
        executor.shutdown();

        boolean terminated =
                executor.awaitTermination(
                        2,
                        TimeUnit.SECONDS
                );

        // Assert
        assertAll(
                () -> assertTrue(terminated),
                () -> assertTrue(executor.isShutdown()),
                () -> assertTrue(executor.isTerminated()),
                () -> assertFalse(task.isCompletedExceptionally()),
                () -> assertEquals("COMPLETED", task.join())
        );
    }

    private GatewayService createGateway(
            SecurityTokenClient securityClient,
            UserDataClient userDataClient,
            Duration timeout
    ) {
        return new GatewayService(
                executor,
                securityClient,
                userDataClient,
                timeout
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
