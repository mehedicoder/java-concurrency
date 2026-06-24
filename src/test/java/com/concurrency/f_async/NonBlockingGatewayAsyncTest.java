package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NonBlockingGatewayAsyncTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();

        assertTrue(
                executor.awaitTermination(2, TimeUnit.SECONDS),
                "Executor did not terminate"
        );
    }

    @Test
    void processUserRequestShouldReturnSuccessfulResponse() {
        UserDataClient client = new TestUserDataClient(
                "mehedi",
                "TestMehediDataClient",
                "/api/v1/users/mehedi",
                "UserMetadata[Mehedi, GoldTier]"
        );

        SecurityTokenClient securityClient =
                request -> new AuthenticationToken(
                        request.userId(),
                        "TOKEN_" + request.userId()
                );

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                List.of(client),
                Duration.ofSeconds(2)
        );

        GatewayResponse response =
                gateway.processUserRequest(client).join();

        assertAll(
                () -> assertEquals("mehedi", response.userId()),
                () -> assertEquals(200, response.statusCode()),
                () -> assertTrue(
                        response.body().contains("\"status\": \"SUCCESS\"")
                ),
                () -> assertTrue(
                        response.body().contains("\"userId\": \"mehedi\"")
                ),
                () -> assertTrue(
                        response.body().contains(
                                "\"route\": \"/api/v1/users/mehedi\""
                        )
                ),
                () -> assertTrue(
                        response.body().contains(
                                "\"payload\": "
                                        + "\"UserMetadata[Mehedi, GoldTier]\""
                        )
                )
        );
    }

    @Test
    void processUserRequestShouldPassAuthenticationTokenToDataClient() {
        AtomicReference<String> receivedToken =
                new AtomicReference<>();

        UserDataClient client = new UserDataClient() {

            @Override
            public String userId() {
                return "mehedi";
            }

            @Override
            public String clientName() {
                return "RecordingDataClient";
            }

            @Override
            public UserRequest createRequest() {
                return new UserRequest(
                        "mehedi",
                        "/api/v1/users/mehedi"
                );
            }

            @Override
            public String fetchUserData(
                    String authenticationToken
            ) {
                receivedToken.set(authenticationToken);
                return "Mehedi data";
            }
        };

        SecurityTokenClient securityClient =
                request -> new AuthenticationToken(
                        request.userId(),
                        "AUTH_TOKEN_MEHEDI"
                );

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                List.of(client),
                Duration.ofSeconds(2)
        );

        GatewayResponse response =
                gateway.processUserRequest(client).join();

        assertEquals(200, response.statusCode());
        assertEquals(
                "AUTH_TOKEN_MEHEDI",
                receivedToken.get()
        );
    }

    @Test
    void processAllUserRequestsShouldProcessEveryConfiguredClient() {
        List<UserDataClient> clients = List.of(
                new TestUserDataClient(
                        "mehedi",
                        "MehediTestClient",
                        "/users/mehedi",
                        "Mehedi data"
                ),
                new TestUserDataClient(
                        "sarah",
                        "SarahTestClient",
                        "/users/sarah",
                        "Sarah data"
                ),
                new TestUserDataClient(
                        "daniel",
                        "DanielTestClient",
                        "/users/daniel",
                        "Daniel data"
                )
        );

        SecurityTokenClient securityClient =
                request -> new AuthenticationToken(
                        request.userId(),
                        "TOKEN_" + request.userId()
                );

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                clients,
                Duration.ofSeconds(2)
        );

        List<CompletableFuture<GatewayResponse>> futures =
                gateway.processAllUserRequests();

        CompletableFuture.allOf(
                futures.toArray(CompletableFuture[]::new)
        ).join();

        List<GatewayResponse> responses = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        assertEquals(3, responses.size());

        assertEquals(
                List.of("mehedi", "sarah", "daniel"),
                responses.stream()
                        .map(GatewayResponse::userId)
                        .toList()
        );

        assertTrue(
                responses.stream()
                        .allMatch(response ->
                                response.statusCode() == 200
                        )
        );
    }

    @Test
    void processUserRequestShouldFailWhenTokenIsBlank() {
        UserDataClient client = validClient();

        SecurityTokenClient securityClient =
                request -> new AuthenticationToken(
                        request.userId(),
                        " "
                );

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        AuthenticationException cause =
                assertInstanceOf(
                        AuthenticationException.class,
                        rootCause(exception)
                );

        assertEquals(
                "Authentication service returned "
                        + "an invalid token for user: mehedi",
                cause.getMessage()
        );
    }

    @Test
    void processUserRequestShouldFailWhenSecurityClientReturnsNull() {
        UserDataClient client = validClient();

        SecurityTokenClient securityClient =
                request -> null;

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        AuthenticationException cause =
                assertInstanceOf(
                        AuthenticationException.class,
                        rootCause(exception)
                );

        assertTrue(
                cause.getMessage().contains(
                        "invalid token for user: mehedi"
                )
        );
    }

    @Test
    void processUserRequestShouldFailWhenTokenBelongsToDifferentUser() {
        UserDataClient client = validClient();

        SecurityTokenClient securityClient =
                request -> new AuthenticationToken(
                        "another-user",
                        "VALID_TOKEN"
                );

        GatewayService gateway = new GatewayService(
                executor,
                securityClient,
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        AuthenticationException cause =
                assertInstanceOf(
                        AuthenticationException.class,
                        rootCause(exception)
                );

        assertEquals(
                "Authentication token belongs to a different user.",
                cause.getMessage()
        );
    }

    @Test
    void processUserRequestShouldFailWhenDownstreamReturnsNull() {
        UserDataClient client = new TestUserDataClient(
                "mehedi",
                "EmptyDataClient",
                "/users/mehedi",
                null
        );

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        DownstreamServiceException cause =
                assertInstanceOf(
                        DownstreamServiceException.class,
                        rootCause(exception)
                );

        assertEquals(
                "EmptyDataClient returned empty user data.",
                cause.getMessage()
        );
    }

    @Test
    void processUserRequestShouldFailWhenDownstreamReturnsBlankData() {
        UserDataClient client = new TestUserDataClient(
                "mehedi",
                "BlankDataClient",
                "/users/mehedi",
                "   "
        );

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        assertInstanceOf(
                DownstreamServiceException.class,
                rootCause(exception)
        );
    }

    @Test
    void processUserRequestShouldPropagateDownstreamException() {
        IllegalStateException downstreamFailure =
                new IllegalStateException(
                        "External service unavailable"
                );

        UserDataClient client = new UserDataClient() {

            @Override
            public String userId() {
                return "mehedi";
            }

            @Override
            public String clientName() {
                return "FailingDataClient";
            }

            @Override
            public UserRequest createRequest() {
                return new UserRequest(
                        "mehedi",
                        "/users/mehedi"
                );
            }

            @Override
            public String fetchUserData(String token) {
                throw downstreamFailure;
            }
        };

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(client).join()
        );

        assertSame(
                downstreamFailure,
                rootCause(exception)
        );
    }

    @Test
    void processUserRequestShouldTimeoutWhenPipelineIsTooSlow() {
        UserDataClient slowClient = new UserDataClient() {

            @Override
            public String userId() {
                return "slow-user";
            }

            @Override
            public String clientName() {
                return "SlowDataClient";
            }

            @Override
            public UserRequest createRequest() {
                return new UserRequest(
                        "slow-user",
                        "/users/slow-user"
                );
            }

            @Override
            public String fetchUserData(String token) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(exception);
                }

                return "Slow data";
            }
        };

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(slowClient),
                Duration.ofMillis(50)
        );

        CompletionException exception = assertThrows(
                CompletionException.class,
                () -> gateway.processUserRequest(slowClient).join()
        );

        assertInstanceOf(
                TimeoutException.class,
                rootCause(exception)
        );
    }

    @Test
    void processUserRequestShouldRejectNullClient() {
        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(validClient()),
                Duration.ofSeconds(2)
        );

        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> gateway.processUserRequest(null)
        );

        assertEquals(
                "userDataClient must not be null",
                exception.getMessage()
        );
    }

    @Test
    void processUserRequestShouldRejectNullRequest() {
        UserDataClient client = new UserDataClient() {

            @Override
            public String userId() {
                return "mehedi";
            }

            @Override
            public String clientName() {
                return "NullRequestClient";
            }

            @Override
            public UserRequest createRequest() {
                return null;
            }

            @Override
            public String fetchUserData(String token) {
                return "data";
            }
        };

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> gateway.processUserRequest(client)
        );

        assertEquals(
                "User data client returned a null request.",
                exception.getMessage()
        );
    }

    @Test
    void processUserRequestShouldRejectBlankUserId() {
        UserDataClient client = clientReturningRequest(
                new UserRequest(
                        " ",
                        "/users/mehedi"
                )
        );

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.processUserRequest(client)
        );

        assertEquals(
                "User ID is required.",
                exception.getMessage()
        );
    }

    @Test
    void processUserRequestShouldRejectBlankRoute() {
        UserDataClient client = clientReturningRequest(
                new UserRequest(
                        "mehedi",
                        " "
                )
        );

        GatewayService gateway = gatewayWithValidSecurityClient(
                List.of(client),
                Duration.ofSeconds(2)
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> gateway.processUserRequest(client)
        );

        assertEquals(
                "Gateway route is required.",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectNullExecutor() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        null,
                        validSecurityClient(),
                        List.of(validClient()),
                        Duration.ofSeconds(1)
                )
        );

        assertEquals(
                "executor must not be null",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectNullSecurityClient() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        null,
                        List.of(validClient()),
                        Duration.ofSeconds(1)
                )
        );

        assertEquals(
                "securityClient must not be null",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectNullClientList() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        null,
                        Duration.ofSeconds(1)
                )
        );

        assertEquals(
                "userDataClients must not be null",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectEmptyClientList() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        List.of(),
                        Duration.ofSeconds(1)
                )
        );

        assertEquals(
                "At least one user-data client is required.",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectListContainingNullClient() {
        List<UserDataClient> clients =
                java.util.Arrays.asList(
                        validClient(),
                        null
                );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        clients,
                        Duration.ofSeconds(1)
                )
        );

        assertEquals(
                "userDataClients must not contain null.",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectNullTimeout() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        List.of(validClient()),
                        null
                )
        );

        assertEquals(
                "requestTimeout must not be null",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectZeroTimeout() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        List.of(validClient()),
                        Duration.ZERO
                )
        );

        assertEquals(
                "Request timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    void constructorShouldRejectNegativeTimeout() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayService(
                        executor,
                        validSecurityClient(),
                        List.of(validClient()),
                        Duration.ofMillis(-1)
                )
        );

        assertEquals(
                "Request timeout must be positive.",
                exception.getMessage()
        );
    }

    private GatewayService gatewayWithValidSecurityClient(
            List<UserDataClient> clients,
            Duration timeout
    ) {
        return new GatewayService(
                executor,
                validSecurityClient(),
                clients,
                timeout
        );
    }

    private SecurityTokenClient validSecurityClient() {
        return request -> new AuthenticationToken(
                request.userId(),
                "TOKEN_" + request.userId()
        );
    }

    private UserDataClient validClient() {
        return new TestUserDataClient(
                "mehedi",
                "TestDataClient",
                "/users/mehedi",
                "Mehedi data"
        );
    }

    private UserDataClient clientReturningRequest(
            UserRequest request
    ) {
        return new UserDataClient() {

            @Override
            public String userId() {
                return request.userId();
            }

            @Override
            public String clientName() {
                return "RequestTestClient";
            }

            @Override
            public UserRequest createRequest() {
                return request;
            }

            @Override
            public String fetchUserData(String token) {
                return "data";
            }
        };
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

    private static final class TestUserDataClient
            implements UserDataClient {

        private final String userId;
        private final String clientName;
        private final String route;
        private final String userData;

        private TestUserDataClient(
                String userId,
                String clientName,
                String route,
                String userData
        ) {
            this.userId = userId;
            this.clientName = clientName;
            this.route = route;
            this.userData = userData;
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
        public UserRequest createRequest() {
            return new UserRequest(
                    userId,
                    route
            );
        }

        @Override
        public String fetchUserData(
                String authenticationToken
        ) {
            return userData;
        }
    }
}