package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class UserRequestProcessor {

    // 1. instead of ThreadLocal: Declare an immutable, safe capability key
    public static final ScopedValue<String> CONTEXT_USER = ScopedValue.newInstance();

    private final DetailsClient detailsClient;

    public UserRequestProcessor(DetailsClient detailsClient) {
        this.detailsClient = detailsClient;
    }

    /**
     * Entry point setting up the dynamic scope context.
     */
    public String processRequest(String userId) throws Exception {
        // 2. BIND VALUE: ScopedValue.where establishes a clear execution lifecycle bounds block.
        // It runs the lambda argument while ensuring the binding automatically cleans up on exit.
        return ScopedValue.where(CONTEXT_USER, userId)
                .call(() -> executeStructuredSubtasks());
    }

    /**
     * Orchestrates concurrent branches that seamlessly inherit the bounded ScopedValue context.
     */
    private String executeStructuredSubtasks() throws Exception {
        // Open structured task framework block
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                cfg -> cfg.withTimeout(Duration.ofSeconds(3)))) {

            // 3. AUTOMATIC INHERITANCE: The forked Virtual Thread automatically captures
            // the exact snapshot bindings established by the parent caller.
            Subtask<String> detailsSubtask = scope.fork(() -> {
                // This call safely unboxes the token string. No more null surprises!
                String currentUser = CONTEXT_USER.get();
                return detailsClient.fetchDetails(currentUser);
            });

            scope.join();
            return detailsSubtask.get();
        }
    }
}

interface DetailsClient {
    String fetchDetails(String username) throws Exception;
}