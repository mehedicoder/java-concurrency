package com.concurrency.g_structured;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

/**
 * Demonstrates structured request-context propagation using ScopedValue.
 *
 * The parent request binds a user identity once, and the structured child task
 * automatically inherits that immutable context.
 *
 * Orchestration:
 * Single structured child task with scoped context inheritance,
 * shared timeout, failure propagation, and bounded task lifetime.
 */
public final class F_UserRequestProcessor {

    private static final Duration REQUEST_TIMEOUT =
            Duration.ofSeconds(3);

    /**
     * Immutable request-context key.
     *
     * The value is bound only for the lifetime of one request and is
     * automatically inherited by child tasks created in the structured scope.
     */
    public static final ScopedValue<String> CONTEXT_USER =
            ScopedValue.newInstance();

    private final DetailsClient detailsClient;

    public F_UserRequestProcessor(DetailsClient detailsClient) {
        this.detailsClient =
                Objects.requireNonNull(
                        detailsClient,
                        "detailsClient must not be null"
                );
    }

    /**
     * Entry point for processing one user request.
     *
     * Binds the user ID to the current execution scope and automatically
     * removes the binding when the request finishes.
     */
    public String processRequest(String userId)
            throws Exception {

        Objects.requireNonNull(
                userId,
                "userId must not be null"
        );

        if (userId.isBlank()) {
            throw new IllegalArgumentException(
                    "userId must not be blank"
            );
        }

        // Bind immutable request context for the complete workflow lifecycle.
        // The binding is visible only inside this call and is cleaned up on exit.
        return ScopedValue
                .where(CONTEXT_USER, userId)
                .call(this::fetchUserDetails);
    }

    /**
     * Executes the downstream details request inside a structured task scope.
     *
     * The child task automatically inherits the CONTEXT_USER binding from
     * the parent request.
     */
    private String fetchUserDetails()
            throws Exception {

        // Structured scope with all-success semantics.
        // Orchestration: one bounded child task with automatic context inheritance.
        // Configurable shared request timeout and structured failure propagation.
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner
                        .allSuccessfulOrThrow(),
                configuration -> configuration
                        .withName("user-details-request")
                        .withTimeout(REQUEST_TIMEOUT)
        )) {
            // Fork the downstream request into a scoped child virtual thread.
            // CONTEXT_USER is inherited automatically from the parent binding.
            Subtask<String> detailsTask =
                    scope.fork(
                            () -> detailsClient.fetchDetails(
                                    CONTEXT_USER.get()
                            )
                    );

            // Synchronization barrier: wait for successful completion,
            // failure, interruption, or timeout.
            scope.join();

            // Safe after a successful join because the child task completed.
            return detailsTask.get();

        } catch (
                StructuredTaskScope.FailedException exception
        ) {
            // Unpack and rethrow the original domain error so callers
            // can handle the real downstream failure.
            Throwable cause = exception.getCause();

            if (cause instanceof Exception businessException) {
                throw businessException;
            }

            throw new RuntimeException(
                    "Unexpected failure while fetching user details",
                    cause
            );
        }
    }
}

/**
 * Low-level dependency abstraction to support clean mocking or stubbing
 * inside unit and integration tests.
 */
interface DetailsClient {

    String fetchDetails(String userId)
            throws Exception;
}