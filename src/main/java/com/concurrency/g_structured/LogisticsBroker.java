package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.StructuredTaskScope;

public class LogisticsBroker {

    private final ApiClient apiClient;
    private final ThreadFactory virtualThreadFactory;

    public LogisticsBroker(ApiClient apiClient, ThreadFactory virtualThreadFactory) {
        this.apiClient = apiClient;
        this.virtualThreadFactory = virtualThreadFactory;
    }

    public record DeliveryConsensus(String carrierRoute, double baseQuote) {}

    public DeliveryConsensus fetchFastestConsensus(String trackingCode, Duration timeout) throws Exception {
        // Joiner.anySuccessfulResultOrThrow immediately returns the first non-faulty response
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<DeliveryConsensus>anySuccessfulResultOrThrow(),
                cfg -> cfg.withThreadFactory(virtualThreadFactory).withTimeout(timeout))) {

            // Forking multiple network calls asynchronously using lightweight Virtual Threads
            scope.fork(() -> apiClient.fetchRouteFromFedex(trackingCode));
            scope.fork(() -> apiClient.fetchRouteFromUps(trackingCode));

            // join() cleanly returns the winner's payload object, canceling any remaining tasks
            return scope.join();

        } catch (StructuredTaskScope.FailedException e) {
            if (e.getCause() instanceof Exception businessEx) throw businessEx;
            throw new RuntimeException(e);
        }
    }
}

interface ApiClient {
    LogisticsBroker.DeliveryConsensus fetchRouteFromFedex(String code) throws Exception;
    LogisticsBroker.DeliveryConsensus fetchRouteFromUps(String code) throws Exception;
}