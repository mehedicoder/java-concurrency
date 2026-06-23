package com.concurrency.f_async;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class E_NonBlockingGatewayAsync {

    public static void main(String[] args) {
        ExecutorService executor =
                Executors.newFixedThreadPool(4);

        try {
            SecurityTokenClient securityClient =
                    route -> "AUTH_TOKEN_XYZ_123";

            UserDataClient userDataClient =
                    token -> "UserMetadata[Mehedi, GoldTier]";

            GatewayService gateway =
                    new GatewayService(
                            executor,
                            securityClient,
                            userDataClient,
                            Duration.ofSeconds(2)
                    );

            System.out.println(
                    "[Gateway Routing Core] Main engine initialized."
            );

            CompletableFuture<GatewayResponse> responseFuture =
                    gateway.processIncomingHttpRequest(
                            "/api/v1/user/profile"
                    );

            responseFuture.whenComplete((response, error) -> {
                if (error != null) {
                    System.err.println(
                            "[Gateway] Request failed: "
                                    + rootCause(error).getMessage()
                    );
                } else {
                    System.out.println(
                            "[Gateway Outbound Routing] HTTP "
                                    + response.statusCode()
                                    + " dispatched to client: "
                                    + response.body()
                    );
                }
            });

            System.out.println(
                    "[Gateway Routing Core] Request accepted; "
                            + "the caller thread remains available."
            );

            /*
             * Console demonstration only.
             * A real web server remains alive independently.
             */
            responseFuture.join();

        } finally {
            shutdownExecutor(executor);
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
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

final class GatewayService {

    private final ExecutorService executor;
    private final SecurityTokenClient securityClient;
    private final UserDataClient userDataClient;
    private final Duration requestTimeout;

    GatewayService(
            ExecutorService executor,
            SecurityTokenClient securityClient,
            UserDataClient userDataClient,
            Duration requestTimeout
    ) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.securityClient = Objects.requireNonNull(
                securityClient,
                "securityClient must not be null"
        );

        this.userDataClient = Objects.requireNonNull(
                userDataClient,
                "userDataClient must not be null"
        );

        this.requestTimeout = requirePositiveTimeout(
                requestTimeout
        );
    }

    CompletableFuture<GatewayResponse> processIncomingHttpRequest(
            String route
    ) {
        validateRoute(route);

        return fetchSecurityTokenAsync(route)
                .thenCompose(this::fetchUserDataAsync)
                .thenApply(userData ->
                        new GatewayResponse(
                                200,
                                """
                                {
                                  "status": "SUCCESS",
                                  "payload": "%s"
                                }
                                """.formatted(userData).strip()
                        )
                )
                .orTimeout(
                        requestTimeout.toMillis(),
                        TimeUnit.MILLISECONDS
                );
    }

    private CompletableFuture<String> fetchSecurityTokenAsync(
            String route
    ) {
        return CompletableFuture.supplyAsync(
                () -> {
                    String token =
                            securityClient.fetchToken(route);

                    if (token == null || token.isBlank()) {
                        throw new AuthenticationException(
                                "Authentication service returned an invalid token."
                        );
                    }

                    return token;
                },
                executor
        );
    }

    private CompletableFuture<String> fetchUserDataAsync(
            String token
    ) {
        return CompletableFuture.supplyAsync(
                () -> {
                    String userData =
                            userDataClient.fetchUserData(token);

                    if (userData == null || userData.isBlank()) {
                        throw new DownstreamServiceException(
                                "User service returned an empty response."
                        );
                    }

                    return userData;
                },
                executor
        );
    }

    private static void validateRoute(String route) {
        if (route == null || route.isBlank()) {
            throw new IllegalArgumentException(
                    "Gateway route is required."
            );
        }
    }

    private static Duration requirePositiveTimeout(
            Duration timeout
    ) {
        Objects.requireNonNull(
                timeout,
                "requestTimeout must not be null"
        );

        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException(
                    "Request timeout must be positive."
            );
        }

        return timeout;
    }
}

@FunctionalInterface
interface SecurityTokenClient {

    String fetchToken(String route);
}

@FunctionalInterface
interface UserDataClient {

    String fetchUserData(String token);
}

record GatewayResponse(
        int statusCode,
        String body
) {
    GatewayResponse {
        Objects.requireNonNull(
                body,
                "body must not be null"
        );
    }
}

class AuthenticationException extends RuntimeException {

    AuthenticationException(String message) {
        super(message);
    }
}

class DownstreamServiceException extends RuntimeException {

    DownstreamServiceException(String message) {
        super(message);
    }
}