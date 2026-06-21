package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.stream.Stream;

public class CryptoTokenService {

    private final HashEngine hashEngine;
    private final ThreadFactory platformThreadFactory;

    // Dependency injection makes this fully mockable and testable
    public CryptoTokenService(HashEngine hashEngine, ThreadFactory platformThreadFactory) {
        this.hashEngine = hashEngine;
        this.platformThreadFactory = platformThreadFactory;
    }

    public record SecurityPayload(String blockA, String blockB) {}

    public SecurityPayload generateSecureTokens(String saltData, Duration timeout) throws Exception {
        // Force platform threads to pin tasks safely onto separate heavy OS threads
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