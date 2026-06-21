package com.concurrency.g_structured;

import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class SecurityContextAuditor {

    public static final ScopedValue<String> PRINCIPAL = ScopedValue.newInstance();
    public static final ScopedValue<String> ROLE = ScopedValue.newInstance();

    public String authorizeAndExecute() throws Exception {
        // Multi-binding composition syntax
        return ScopedValue.where(PRINCIPAL, "admin_user")
                .where(ROLE, "SUPER_USER")
                .call(() -> runAuditedTasks());
    }

    private String runAuditedTasks() throws Exception {
        try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.allSuccessfulOrThrow())) {

            Subtask<String> auditLog = scope.fork(() -> {
                // Accessing both values inside child virtual thread execution structures
                return String.format("Audit Entry: User [%s] holding role [%s] cleared checkpoint.",
                        PRINCIPAL.get(), ROLE.get());
            });

            scope.join();
            return auditLog.get();
        }
    }

    public static void main(String[] args) throws Exception {
        String logResult = new SecurityContextAuditor().authorizeAndExecute();
        System.out.println(logResult);
    }
}