package com.concurrency.g_structured;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;

/*
* Demonstrate first-success-wins orchestration, others are canceled automatically.
 */
public class C_LogisticsBroker {

    private final ApiClient apiClient;
    private final ThreadFactory virtualThreadFactory;

    public C_LogisticsBroker(
            ApiClient apiClient,
            ThreadFactory virtualThreadFactory
    ) {
        this.apiClient =
                Objects.requireNonNull(apiClient);

        this.virtualThreadFactory =
                Objects.requireNonNull(virtualThreadFactory);
    }

    public record DeliveryQuote(
            String carrier,
            String route,
            double baseQuote
    ) {
    }

    public DeliveryQuote fetchFastestSuccessfulQuote(
            String trackingCode,
            Duration timeout
    ) throws Exception {

        Objects.requireNonNull(trackingCode);
        Objects.requireNonNull(timeout);

        // Orchestration: if one task succeeds, others are canceled automatically.
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner
                        .<DeliveryQuote>
                                anySuccessfulResultOrThrow(),
                configuration -> configuration
                        .withThreadFactory(
                                virtualThreadFactory
                        )
                        .withTimeout(timeout)
        )) {
            scope.fork(
                    () -> apiClient.fetchRouteFromFedex(
                            trackingCode
                    )
            );

            scope.fork(
                    () -> apiClient.fetchRouteFromUps(
                            trackingCode
                    )
            );

            /*
             * Returns the first successful result.
             * Any unfinished sibling task is cancelled.
             */
            return scope.join();

        } catch (
                StructuredTaskScope.FailedException exception
        ) {
            Throwable cause = exception.getCause();

            if (cause instanceof Exception businessException) {
                throw businessException;
            }

            throw new RuntimeException(cause);
        }
    }
}

interface ApiClient {

    C_LogisticsBroker.DeliveryQuote fetchRouteFromFedex(
            String trackingCode
    ) throws Exception;

    C_LogisticsBroker.DeliveryQuote fetchRouteFromUps(
            String trackingCode
    ) throws Exception;
}