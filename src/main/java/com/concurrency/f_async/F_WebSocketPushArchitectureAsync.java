package com.concurrency.f_async;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        8,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName(
                                    "websocket-worker-"
                                            + THREAD_COUNTER.incrementAndGet()
                            );
                            return thread;
                        }
                );

        try {
            AuthenticationService authenticationService =
                    new SimulatedAuthenticationService(executor);

            PermissionService permissionService =
                    new SimulatedPermissionService(executor);

            SessionRepository sessionRepository =
                    new SimulatedSessionRepository(executor);

            WebSocketRegistrationService registrationService =
                    new WebSocketRegistrationService(
                            executor,
                            authenticationService,
                            permissionService,
                            sessionRepository,
                            rawMessage -> {
                                ThreadLog.log(
                                        "Processing incoming WebSocket payload."
                                );

                                sleep(Duration.ofMillis(400));

                                return "Processed JSON Payload: "
                                        + rawMessage.toUpperCase(
                                        Locale.ROOT
                                );
                            },
                            message -> ThreadLog.log(
                                    "Pushing message to client: " + message
                            )
                    );

            ThreadLog.log("Application started.");

            CompletableFuture<SessionHandle> registrationFuture =
                    registrationService.registerSessionAsync(
                            "WS-SESSION-99X",
                            "VALID-TOKEN-123"
                    );

            ThreadLog.log(
                    "Registration request returned immediately."
            );

            ThreadLog.log(
                    "Main thread can accept another connection."
            );

            CompletableFuture<String> finalPushResult =
                    registrationFuture.thenCompose(sessionHandle -> {
                        ThreadLog.log(
                                "Registration completed for user "
                                        + sessionHandle.userId()
                        );

                        ThreadLog.log(
                                "Permissions: "
                                        + sessionHandle.permissions()
                        );

                        boolean delivered =
                                registrationService.onDataReceived(
                                        sessionHandle.sessionId(),
                                        """
                                        {
                                          "action": "checkout",
                                          "items": 3
                                        }
                                        """
                                );

                        ThreadLog.log(
                                "Network event delivered: " + delivered
                        );

                        if (!delivered) {
                            throw new IllegalStateException(
                                    "The registered session was not found."
                            );
                        }

                        return sessionHandle.messageResult();
                    });

            ThreadLog.log(
                    "Main thread reached join and now waits "
                            + "only to keep the console application alive."
            );

            String result = finalPushResult.join();

            ThreadLog.log("Final push result: " + result);

        } catch (CompletionException exception) {
            ThreadLog.log(
                    "Registration or push failed: "
                            + rootCause(exception).getMessage()
            );
        } finally {
            shutdownExecutor(executor);
        }
    }

    private static final AtomicInteger THREAD_COUNTER =
            new AtomicInteger();

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CompletionException(exception);
        }
    }

    private static String thread() {
        return "[" + Thread.currentThread().getName() + "]";
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
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

final class WebSocketRegistrationService {

    private final ConcurrentHashMap<String, PendingSession>
            activeSessions = new ConcurrentHashMap<>();

    private final ExecutorService executor;
    private final AuthenticationService authenticationService;
    private final PermissionService permissionService;
    private final SessionRepository sessionRepository;
    private final Function<String, String> messageProcessor;
    private final Consumer<String> outboundSender;

    public WebSocketRegistrationService(
            ExecutorService executor,
            AuthenticationService authenticationService,
            PermissionService permissionService,
            SessionRepository sessionRepository,
            Function<String, String> messageProcessor,
            Consumer<String> outboundSender
    ) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.authenticationService = Objects.requireNonNull(
                authenticationService,
                "authenticationService must not be null"
        );

        this.permissionService = Objects.requireNonNull(
                permissionService,
                "permissionService must not be null"
        );

        this.sessionRepository = Objects.requireNonNull(
                sessionRepository,
                "sessionRepository must not be null"
        );

        this.messageProcessor = Objects.requireNonNull(
                messageProcessor,
                "messageProcessor must not be null"
        );

        this.outboundSender = Objects.requireNonNull(
                outboundSender,
                "outboundSender must not be null"
        );
    }

    CompletableFuture<SessionHandle> registerSessionAsync(
            String sessionId,
            String accessToken
    ) {
        validateSessionId(sessionId);
        validateAccessToken(accessToken);

        /*
         * Step 1:
         * Validate the access token asynchronously.
         */
        return authenticationService
                .authenticateAsync(accessToken)

                /*
                 * Step 2:
                 * After authentication succeeds, load permissions.
                 */
                .thenCompose(authenticatedUser ->
                        permissionService
                                .loadPermissionsAsync(
                                        authenticatedUser.userId()
                                )
                                .thenApply(permissions ->
                                        new AuthenticatedSessionData(
                                                authenticatedUser,
                                                permissions
                                        )
                                )
                )

                /*
                 * Step 3:
                 * Persist the session in a remote session store.
                 */
                .thenCompose(sessionData ->
                        sessionRepository
                                .saveAsync(
                                        new PersistedSession(
                                                sessionId,
                                                sessionData.user().userId(),
                                                sessionData.permissions()
                                        )
                                )
                                .thenApply(ignored -> sessionData)
                )

                /*
                 * Step 4:
                 * Register the local one-shot message pipeline.
                 */
                .thenApply(sessionData ->
                        registerLocalSession(
                                sessionId,
                                sessionData
                        )
                );
    }

    private SessionHandle registerLocalSession(
            String sessionId,
            AuthenticatedSessionData sessionData
    ) {
        ThreadLog.log(
                "Registering local WebSocket session: "
                        + sessionId
        );

        CompletableFuture<String> incomingMessage =
                new CompletableFuture<>();

        CompletableFuture<String> processingPipeline =
                incomingMessage
                        .thenApplyAsync(
                                rawMessage -> {
                                    ThreadLog.log(
                                            "Message processing stage started."
                                    );

                                    String processed =
                                            messageProcessor.apply(rawMessage);

                                    ThreadLog.log(
                                            "Message processing stage completed."
                                    );

                                    return processed;
                                },
                                executor
                        )
                        .thenApply(processedMessage -> {
                            ThreadLog.log(
                                    "Outbound push stage started."
                            );

                            outboundSender.accept(processedMessage);

                            ThreadLog.log(
                                    "Outbound push stage completed."
                            );

                            return processedMessage;
                        });

        PendingSession pendingSession =
                new PendingSession(
                        incomingMessage,
                        processingPipeline
                );

        PendingSession existing =
                activeSessions.putIfAbsent(
                        sessionId,
                        pendingSession
                );

        if (existing != null) {
            throw new DuplicateSessionException(
                    "Session is already registered: "
                            + sessionId
            );
        }

        ThreadLog.log(
                "Local session registration completed."
        );

        processingPipeline.whenComplete(
                (result, error) -> {
                    activeSessions.remove(
                            sessionId,
                            pendingSession
                    );

                    ThreadLog.log(
                            "Session removed from active registry: "
                                    + sessionId
                    );
                }
        );

        return new SessionHandle(
                sessionId,
                sessionData.user().userId(),
                sessionData.permissions(),
                processingPipeline
        );
    }

    boolean onDataReceived(
            String sessionId,
            String rawPayload
    ) {
        validateSessionId(sessionId);
        validatePayload(rawPayload);

        ThreadLog.log(
                "Network data received for session: "
                        + sessionId
        );

        PendingSession pendingSession =
                activeSessions.remove(sessionId);

        if (pendingSession == null) {
            ThreadLog.log(
                    "No active session found."
            );
            return false;
        }

        boolean completed =
                pendingSession
                        .incomingMessage()
                        .complete(rawPayload);

        ThreadLog.log(
                "Incoming-message future completed: "
                        + completed
        );

        return completed;
    }

    CompletableFuture<Boolean> disconnectSessionAsync(
            String sessionId
    ) {
        validateSessionId(sessionId);

        PendingSession pendingSession =
                activeSessions.remove(sessionId);

        if (pendingSession == null) {
            return CompletableFuture.completedFuture(false);
        }

        boolean pipelineCancelled =
                pendingSession.processingPipeline().cancel(false);

        boolean incomingCancelled =
                pendingSession.incomingMessage().cancel(false);

        ThreadLog.log(
                "Disconnecting %s; pipelineCancelled=%s, incomingCancelled=%s"
                        .formatted(
                                sessionId,
                                pipelineCancelled,
                                incomingCancelled
                        )
        );

        return sessionRepository
                .deleteAsync(sessionId)
                .thenApply(ignored -> true);
    }

    int activeSessionCount() {
        return activeSessions.size();
    }

    private static void validateSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException(
                    "Session ID is required."
            );
        }
    }

    private static void validateAccessToken(
            String accessToken
    ) {
        if (accessToken == null || accessToken.isBlank()) {
            throw new IllegalArgumentException(
                    "Access token is required."
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

record SessionHandle(
        String sessionId,
        String userId,
        Set<String> permissions,
        CompletableFuture<String> messageResult
) {
    SessionHandle {
        Objects.requireNonNull(
                sessionId,
                "sessionId must not be null"
        );

        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        permissions = Set.copyOf(
                Objects.requireNonNull(
                        permissions,
                        "permissions must not be null"
                )
        );

        Objects.requireNonNull(
                messageResult,
                "messageResult must not be null"
        );
    }
}

record PendingSession(
        CompletableFuture<String> incomingMessage,
        CompletableFuture<String> processingPipeline
) {
    PendingSession {
        Objects.requireNonNull(
                incomingMessage,
                "incomingMessage must not be null"
        );

        Objects.requireNonNull(
                processingPipeline,
                "processingPipeline must not be null"
        );
    }
}

record AuthenticatedUser(
        String userId
) {
    AuthenticatedUser {
        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );
    }
}

record AuthenticatedSessionData(
        AuthenticatedUser user,
        Set<String> permissions
) {
    AuthenticatedSessionData {
        Objects.requireNonNull(
                user,
                "user must not be null"
        );

        permissions = Set.copyOf(
                Objects.requireNonNull(
                        permissions,
                        "permissions must not be null"
                )
        );
    }
}

record PersistedSession(
        String sessionId,
        String userId,
        Set<String> permissions
) {
    public PersistedSession {
        Objects.requireNonNull(
                sessionId,
                "sessionId must not be null"
        );

        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        permissions = Set.copyOf(
                Objects.requireNonNull(
                        permissions,
                        "permissions must not be null"
                )
        );
    }
}

@FunctionalInterface
interface AuthenticationService {

    CompletableFuture<AuthenticatedUser> authenticateAsync(
            String accessToken
    );
}

@FunctionalInterface
interface PermissionService {

    CompletableFuture<Set<String>> loadPermissionsAsync(
            String userId
    );
}

interface SessionRepository {

    CompletableFuture<Void> saveAsync(
            PersistedSession session
    );

    CompletableFuture<Void> deleteAsync(
            String sessionId
    );
}

final class SimulatedAuthenticationService
        implements AuthenticationService {

    private final ExecutorService executor;

    SimulatedAuthenticationService(
            ExecutorService executor
    ) {
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public CompletableFuture<AuthenticatedUser>
    authenticateAsync(String accessToken) {

        return CompletableFuture.supplyAsync(
                () -> {
                    sleep(Duration.ofMillis(300));

                    if (!accessToken.startsWith("VALID-")) {
                        throw new AuthenticationFailureException(
                                "Invalid WebSocket access token."
                        );
                    }

                    return new AuthenticatedUser(
                            "USER-MEHEDI-001"
                    );
                },
                executor
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

final class SimulatedPermissionService
        implements PermissionService {

    private final ExecutorService executor;

    SimulatedPermissionService(
            ExecutorService executor
    ) {
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public CompletableFuture<Set<String>>
    loadPermissionsAsync(String userId) {

        ThreadLog.log(
                "Submitting permission-loading operation."
        );

        return CompletableFuture.supplyAsync(
                () -> {
                    ThreadLog.log(
                            "Permission loading started for " + userId
                    );

                    sleep(Duration.ofMillis(250));

                    Set<String> permissions = Set.of(
                            "WEBSOCKET_CONNECT",
                            "CHECKOUT_EVENTS_READ"
                    );

                    ThreadLog.log(
                            "Permission loading completed."
                    );

                    return permissions;
                },
                executor
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

final class SimulatedSessionRepository
        implements SessionRepository {

    private final ExecutorService executor;
    private final ConcurrentHashMap<String, PersistedSession>
            storedSessions = new ConcurrentHashMap<>();

    SimulatedSessionRepository(
            ExecutorService executor
    ) {
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public CompletableFuture<Void> saveAsync(
            PersistedSession session
    ) {
        ThreadLog.log(
                "Submitting session-persistence operation."
        );

        return CompletableFuture.runAsync(
                () -> {
                    ThreadLog.log(
                            "Persisting session "
                                    + session.sessionId()
                    );

                    sleep(Duration.ofMillis(200));

                    storedSessions.put(
                            session.sessionId(),
                            session
                    );

                    ThreadLog.log(
                            "Session persisted successfully."
                    );
                },
                executor
        );
    }

    @Override
    public CompletableFuture<Void> deleteAsync(
            String sessionId
    ) {
        return CompletableFuture.runAsync(
                () -> {
                    sleep(Duration.ofMillis(100));
                    storedSessions.remove(sessionId);
                },
                executor
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

class AuthenticationFailureException
        extends RuntimeException {

    AuthenticationFailureException(String message) {
        super(message);
    }
}

class DuplicateSessionException
        extends RuntimeException {

    DuplicateSessionException(String message) {
        super(message);
    }
}

final class ThreadLog {

    private ThreadLog() {
    }

    static void log(String message) {
        System.out.printf(
                "%-25s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }
}
