package com.concurrency.f_async;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class E_NonBlockingGatewayAsync {

    public static void main(String[] args) {
        ExecutorService executor =
                Executors.newFixedThreadPool(6);

        try {
            SecurityTokenClient securityClient =
                    request -> {
                        log(
                                "Authentication started for "
                                        + request.userId()
                        );

                        sleep(Duration.ofMillis(400));

                        AuthenticationToken token =
                                new AuthenticationToken(
                                        request.userId(),
                                        "AUTH_TOKEN_"
                                                + request.userId()
                                                .toUpperCase()
                                );

                        log(
                                "Authentication completed for "
                                        + request.userId()
                        );

                        return token;
                    };

            List<UserDataClient> userDataClients =
                    List.of(
                            new MehediDataClient(),
                            new SarahDataClient(),
                            new DanielDataClient()
                    );

            GatewayService gateway =
                    new GatewayService(
                            executor,
                            securityClient,
                            userDataClients,
                            Duration.ofSeconds(3)
                    );

            log("Gateway routing engine initialized.");

            /*
             * GatewayService asks every configured user-data client
             * to initiate its user-specific request pipeline.
             */
            List<CompletableFuture<GatewayResponse>>
                    responseFutures =
                    gateway.processAllUserRequests();

            responseFutures.forEach(
                    future -> future.whenComplete(
                            E_NonBlockingGatewayAsync::observeResponse
                    )
            );

            log(
                    "All user-specific requests were initiated. "
                            + "The main thread remains available."
            );

            CompletableFuture.allOf(
                    responseFutures.toArray(
                            CompletableFuture[]::new
                    )
            ).join();

            log("All gateway pipelines completed.");

        } catch (CompletionException exception) {
            log(
                    "Gateway processing failed: "
                            + rootCause(exception).getMessage()
            );
        } finally {
            shutdownExecutor(executor);
        }
    }

    private static void observeResponse(
            GatewayResponse response,
            Throwable error
    ) {
        if (error != null) {
            log(
                    "Request failed: "
                            + rootCause(error).getMessage()
            );
            return;
        }

        log(
                response.userId()
                        + " received HTTP "
                        + response.statusCode()
                        + ":\n"
                        + response.body()
        );
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

    private static Throwable rootCause(
            Throwable throwable
    ) {
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
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

final class GatewayService {

    private final ExecutorService executor;
    private final SecurityTokenClient securityClient;
    private final List<UserDataClient> userDataClients;
    private final Duration requestTimeout;

    GatewayService(
            ExecutorService executor,
            SecurityTokenClient securityClient,
            List<UserDataClient> userDataClients,
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

        Objects.requireNonNull(
                userDataClients,
                "userDataClients must not be null"
        );

        if (userDataClients.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one user-data client is required."
            );
        }

        if (userDataClients.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(
                    "userDataClients must not contain null."
            );
        }

        this.userDataClients =
                List.copyOf(userDataClients);

        this.requestTimeout =
                requirePositiveTimeout(requestTimeout);
    }

    /**
     * Starts one independent asynchronous pipeline for every
     * configured user-data client.
     */
    List<CompletableFuture<GatewayResponse>>
    processAllUserRequests() {
        return userDataClients.stream()
                .map(this::processUserRequest)
                .toList();
    }

    /**
     * A specific UserDataClient initiates its own user request.
     */
    CompletableFuture<GatewayResponse> processUserRequest(
            UserDataClient userDataClient
    ) {
        Objects.requireNonNull(
                userDataClient,
                "userDataClient must not be null"
        );

        UserRequest request =
                userDataClient.createRequest();

        validateRequest(request);

        return fetchSecurityTokenAsync(request)
                .thenCompose(token ->
                        fetchUserDataAsync(
                                userDataClient,
                                token
                        )
                )
                .thenApply(userData ->
                        buildResponse(
                                request,
                                userData
                        )
                )
                .orTimeout(
                        requestTimeout.toMillis(),
                        TimeUnit.MILLISECONDS
                );
    }

    private CompletableFuture<AuthenticationToken>
    fetchSecurityTokenAsync(
            UserRequest request
    ) {
        return CompletableFuture.supplyAsync(
                () -> {
                    AuthenticationToken token =
                            securityClient.fetchToken(request);

                    if (token == null
                            || token.value().isBlank()) {
                        throw new AuthenticationException(
                                "Authentication service returned "
                                        + "an invalid token for user: "
                                        + request.userId()
                        );
                    }

                    if (!request.userId().equals(token.userId())) {
                        throw new AuthenticationException(
                                "Authentication token belongs to "
                                        + "a different user."
                        );
                    }

                    return token;
                },
                executor
        );
    }

    private CompletableFuture<String>
    fetchUserDataAsync(
            UserDataClient userDataClient,
            AuthenticationToken token
    ) {
        return CompletableFuture.supplyAsync(
                () -> {
                    log(
                            "Using "
                                    + userDataClient.clientName()
                                    + " for user "
                                    + userDataClient.userId()
                    );

                    String userData =
                            userDataClient.fetchUserData(
                                    token.value()
                            );

                    if (userData == null
                            || userData.isBlank()) {
                        throw new DownstreamServiceException(
                                userDataClient.clientName()
                                        + " returned empty user data."
                        );
                    }

                    return userData;
                },
                executor
        );
    }

    private GatewayResponse buildResponse(
            UserRequest request,
            String userData
    ) {
        return new GatewayResponse(
                request.userId(),
                200,
                """
                {
                  "status": "SUCCESS",
                  "userId": "%s",
                  "route": "%s",
                  "payload": "%s"
                }
                """.formatted(
                        request.userId(),
                        request.route(),
                        userData
                ).strip()
        );
    }

    private static void validateRequest(
            UserRequest request
    ) {
        Objects.requireNonNull(
                request,
                "User data client returned a null request."
        );

        if (request.userId().isBlank()) {
            throw new IllegalArgumentException(
                    "User ID is required."
            );
        }

        if (request.route().isBlank()) {
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

        if (timeout.isZero()
                || timeout.isNegative()) {
            throw new IllegalArgumentException(
                    "Request timeout must be positive."
            );
        }

        return timeout;
    }

    private static void log(String message) {
        System.out.printf(
                "%-25s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }
}

interface UserDataClient {

    String userId();

    String clientName();

    UserRequest createRequest();

    String fetchUserData(String authenticationToken);
}

final class MehediDataClient
        implements UserDataClient {

    @Override
    public String userId() {
        return "mehedi";
    }

    @Override
    public String clientName() {
        return "MehediDataClient";
    }

    @Override
    public UserRequest createRequest() {
        return new UserRequest(
                userId(),
                "/api/v1/user/mehedi/profile"
        );
    }

    @Override
    public String fetchUserData(
            String authenticationToken
    ) {
        log(
                "Fetching Mehedi data with token "
                        + authenticationToken
        );

        sleep(Duration.ofMillis(900));

        return "UserMetadata[Mehedi, GoldTier]";
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
}

final class SarahDataClient
        implements UserDataClient {

    @Override
    public String userId() {
        return "sarah";
    }

    @Override
    public String clientName() {
        return "SarahDataClient";
    }

    @Override
    public UserRequest createRequest() {
        return new UserRequest(
                userId(),
                "/api/v1/user/sarah/profile"
        );
    }

    @Override
    public String fetchUserData(
            String authenticationToken
    ) {
        log(
                "Fetching Sarah data with token "
                        + authenticationToken
        );

        sleep(Duration.ofMillis(700));

        return "UserMetadata[Sarah, SilverTier]";
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
}

final class DanielDataClient
        implements UserDataClient {

    @Override
    public String userId() {
        return "daniel";
    }

    @Override
    public String clientName() {
        return "DanielDataClient";
    }

    @Override
    public UserRequest createRequest() {
        return new UserRequest(
                userId(),
                "/api/v1/user/daniel/profile"
        );
    }

    @Override
    public String fetchUserData(
            String authenticationToken
    ) {
        log(
                "Fetching Daniel data with token "
                        + authenticationToken
        );

        sleep(Duration.ofMillis(500));

        return "UserMetadata[Daniel, PlatinumTier]";
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
}

@FunctionalInterface
interface SecurityTokenClient {

    AuthenticationToken fetchToken(
            UserRequest request
    );
}

record UserRequest(
        String userId,
        String route
) {
    UserRequest {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        Objects.requireNonNull(
                route,
                "route must not be null"
        );
    }
}

record AuthenticationToken(
        String userId,
        String value
) {
    AuthenticationToken {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        Objects.requireNonNull(
                value,
                "value must not be null"
        );
    }
}

record GatewayResponse(
        String userId,
        int statusCode,
        String body
) {
    GatewayResponse {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        Objects.requireNonNull(
                body,
                "body must not be null"
        );
    }
}

class AuthenticationException
        extends RuntimeException {

    AuthenticationException(String message) {
        super(message);
    }
}

class DownstreamServiceException
        extends RuntimeException {

    DownstreamServiceException(String message) {
        super(message);
    }
}
