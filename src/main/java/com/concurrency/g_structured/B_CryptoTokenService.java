package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.stream.Stream;

/**
* In traditional Java concurrency (like using ExecutorService), asynchronous tasks are "fire-and-forget."
* They have no inherent relationship to the method that started them. Structured Concurrency fixes this
* by treating a group of related tasks as a single unit of work.
*/
public class B_CryptoTokenService {

    private final HashEngine hashEngine;
    private final ThreadFactory platformThreadFactory;

    public B_CryptoTokenService(HashEngine hashEngine, ThreadFactory platformThreadFactory) {
        this.hashEngine = hashEngine;
        this.platformThreadFactory = platformThreadFactory;
    }

    public record SecurityPayload(String blockA, String blockB) {}

    public SecurityPayload generateSecureTokens(String saltData, Duration timeout) throws Exception {
        // Orchestration: if one task fails others fail too.
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                cfg -> cfg.withThreadFactory(platformThreadFactory).withTimeout(timeout))) {

            Subtask<String> taskA = scope.fork(() -> hashEngine.calculateCpuHeavyHash(saltData + "-A"));
            Subtask<String> taskB = scope.fork(() -> hashEngine.calculateCpuHeavyHash(saltData + "-B"));

            // Returns a stream of completed subtasks upon global success
            Stream<Subtask<Object>> resultStream = scope.join();

            return new SecurityPayload(taskA.get(), taskB.get());

        } catch (StructuredTaskScope.FailedException e) {
            if (e.getCause() instanceof Exception businessEx)
                throw businessEx;
            throw new RuntimeException(e);
        }
    }
}

interface HashEngine {
    String calculateCpuHeavyHash(String input) throws Exception;
}