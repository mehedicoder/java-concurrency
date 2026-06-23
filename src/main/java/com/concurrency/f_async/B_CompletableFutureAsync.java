package com.concurrency.f_async;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class FlightPriceAnalyzerNonBlocking {

    private final ExecutorService executor;

    FlightPriceAnalyzerNonBlocking(ExecutorService executor) {
        this.executor = executor;
    }

    CompletableFuture<Double> fetchPriceAsync(
            String airportCode,
            long simulateLatencyMs
    ) {
        return CompletableFuture.supplyAsync(() -> {
            if (airportCode == null || airportCode.isBlank()) {
                throw new IllegalArgumentException(
                        "Invalid airport code provided."
                );
            }

            if (simulateLatencyMs < 0) {
                throw new IllegalArgumentException(
                        "Latency cannot be negative."
                );
            }

            sleep(simulateLatencyMs);

            String normalizedCode =
                    airportCode.toUpperCase(Locale.ROOT);

            System.out.printf(
                    "[%s] Fetching base flight price for %s...%n",
                    Thread.currentThread().getName(),
                    normalizedCode
            );

            return switch (normalizedCode) {
                case "JFK" -> 450.00;
                case "LAX" -> 550.00;
                case "LHR" -> 720.00;
                default -> 300.00;
            };
        }, executor);
    }

    private static void sleep(long latencyMs) {
        if (latencyMs == 0) {
            return;
        }

        try {
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new CompletionException(
                    "Flight-price lookup was interrupted.",
                    e
            );
        }
    }
}

class ExchangeRateProviderNonBlocking {

    private final ExecutorService executor;

    ExchangeRateProviderNonBlocking(ExecutorService executor) {
        this.executor = executor;
    }

    CompletableFuture<Double> fetchExchangeRateAsync(
            String targetCurrency,
            long latencyMs
    ) {
        return CompletableFuture.supplyAsync(() -> {
            if (targetCurrency == null || targetCurrency.isBlank()) {
                throw new IllegalArgumentException(
                        "Target currency is required."
                );
            }

            if (latencyMs < 0) {
                throw new IllegalArgumentException(
                        "Latency cannot be negative."
                );
            }

            sleep(latencyMs);

            System.out.printf(
                    "[%s] Fetching exchange rate for %s...%n",
                    Thread.currentThread().getName(),
                    targetCurrency
            );

            return "EUR".equalsIgnoreCase(targetCurrency)
                    ? 0.92
                    : 1.0;
        }, executor);
    }

    private static void sleep(long latencyMs) {
        if (latencyMs == 0) {
            return;
        }

        try {
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new CompletionException(
                    "Exchange-rate lookup was interrupted.",
                    e
            );
        }
    }
}

public class B_CompletableFutureAsync {

    public static void main(String[] args) {
        ExecutorService flightPricePool =
                Executors.newFixedThreadPool(5);

        try {
            FlightPriceAnalyzerNonBlocking analyzer =
                    new FlightPriceAnalyzerNonBlocking(
                            flightPricePool
                    );

            ExchangeRateProviderNonBlocking rateProvider =
                    new ExchangeRateProviderNonBlocking(
                            flightPricePool
                    );

            CompletableFuture<Double> rateFuture =
                    rateProvider.fetchExchangeRateAsync(
                            "EUR",
                            1_000
                    );

            System.out.println(
                    "\n--- Aggregated Final Conversion Matrix ---"
            );

            CompletableFuture<Void> jfkPipeline =
                    createPipeline(
                            analyzer,
                            rateFuture,
                            "JFK",
                            2_000
                    );

            CompletableFuture<Void> laxPipeline =
                    createPipeline(
                            analyzer,
                            rateFuture,
                            "LAX",
                            3_000
                    );

            CompletableFuture<Void> lhrPipeline =
                    createPipeline(
                            analyzer,
                            rateFuture,
                            "LHR",
                            1_000
                    );

            CompletableFuture<Void> xyzPipeline =
                    createPipeline(
                            analyzer,
                            rateFuture,
                            "XYZ",
                            1_500
                    );

            CompletableFuture<Void> allTasks =
                    CompletableFuture.allOf(
                            jfkPipeline,
                            laxPipeline,
                            lhrPipeline,
                            xyzPipeline
                    );

            allTasks.join();

            System.out.println(
                    "\n[Main Thread] All asynchronous processing completed."
            );

        } catch (CompletionException e) {
            System.err.println(
                    "Asynchronous workflow failed: "
                            + rootCause(e).getMessage()
            );
        } finally {
            shutdownExecutor(flightPricePool);
        }
    }

    private static CompletableFuture<Void> createPipeline(
            FlightPriceAnalyzerNonBlocking analyzer,
            CompletableFuture<Double> rateFuture,
            String airportCode,
            long latencyMs
    ) {
        return analyzer.fetchPriceAsync(
                        airportCode,
                        latencyMs
                )
                .thenAcceptBoth(
                        rateFuture,
                        (price, rate) ->
                                printResult(
                                        airportCode,
                                        price,
                                        rate
                                )
                );
    }

    private static void printResult(
            String code,
            double basePrice,
            double rate
    ) {
        System.out.printf(
                "[%s] %s Base Price: $%.2f USD | Total: €%.2f EUR%n",
                Thread.currentThread().getName(),
                code,
                basePrice,
                basePrice * rate
        );
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

                if (!executor.awaitTermination(
                        2,
                        TimeUnit.SECONDS
                )) {
                    System.err.println(
                            "Executor did not terminate."
                    );
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}