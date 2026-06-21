package com.concurrency.g_structured;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class NestedTenantProcessor {

    public static final ScopedValue<String> TENANT_ID = ScopedValue.newInstance();

    public void processMultiTenantWorkflow() throws Exception {
        // Outer scope level context: Global System Context
        ScopedValue.where(TENANT_ID, "GLOBAL_SYSTEM").run(() -> {
            System.out.format("[Parent Core] Identity: %s\n", TENANT_ID.get());

            try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.allSuccessfulOrThrow())) {

                // Subtask A inherits the outer context
                Subtask<String> taskA = scope.fork(() ->
                        "TaskA sees: " + TENANT_ID.get()
                );

                // Subtask B explicit REBINDING for a specific client downstream
                Subtask<String> taskB = scope.fork(() ->
                        ScopedValue.where(TENANT_ID, "CLIENT_ACME").call(() ->
                                "TaskB (Nested) sees: " + TENANT_ID.get()
                        )
                );

                try {
                    scope.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Workflow interrupted", e);
                }

                System.out.println(" -> " + taskA.get());
                System.out.println(" -> " + taskB.get());
            }

            // Still retains original value back in the parent execution line
            System.out.format("[Parent Core] Identity post-execution: %s\n", TENANT_ID.get());
        });
    }

    public static void main(String[] args) throws Exception {
        new NestedTenantProcessor().processMultiTenantWorkflow();
    }
}