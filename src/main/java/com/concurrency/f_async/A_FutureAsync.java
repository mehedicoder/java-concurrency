package com.concurrency.f_async;

import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class FlightPriceAnalyzer {

    private final ExecutorService executor;

    FlightPriceAnalyzer(ExecutorService executor) {
        this.executor = executor;
    }

    Future<Double> fetchPriceAsync(
            String airportCode,
            long simulateLatencyMs
    ) {
        return executor.submit(() -> {
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

            if (simulateLatencyMs > 0) {
                Thread.sleep(simulateLatencyMs);
            }

            String normalizedCode =
                    airportCode.toUpperCase(Locale.ROOT);

            System.out.printf(
                    "[%s] Fetching price for %s...%n",
                    Thread.currentThread().getName(),
                    normalizedCode
            );

            return switch (normalizedCode) {
                case "JFK" -> 450.00;
                case "LAX" -> 550.00;
                case "LHR" -> 720.00;
                default -> 300.00;
            };
        });
    }
}

class ExchangeRateProvider {

    private final ExecutorService executor;

    ExchangeRateProvider(ExecutorService executor) {
        this.executor = executor;
    }

    Future<Double> fetchExchangeRateAsync(
            String targetCurrency,
            long latencyMs
    ) {
        return executor.submit(() -> {
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

            if (latencyMs > 0) {
                Thread.sleep(latencyMs);
            }

            System.out.printf(
                    "[%s] Fetching exchange rate for %s...%n",
                    Thread.currentThread().getName(),
                    targetCurrency
            );

            return "EUR".equalsIgnoreCase(targetCurrency)
                    ? 0.92
                    : 1.0;
        });
    }
}

public class A_FutureAsync {

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(5);

        try {
            FlightPriceAnalyzer analyzer =
                    new FlightPriceAnalyzer(executor);

            ExchangeRateProvider rateProvider =
                    new ExchangeRateProvider(executor);

            Future<Double> jfk =
                    analyzer.fetchPriceAsync("JFK", 2_000);

            Future<Double> lax =
                    analyzer.fetchPriceAsync("LAX", 3_000);

            Future<Double> lhr =
                    analyzer.fetchPriceAsync("LHR", 1_000);

            Future<Double> xyz =
                    analyzer.fetchPriceAsync("XYZ", 1_500);

            Future<Double> rate =
                    rateProvider.fetchExchangeRateAsync("EUR", 1_000);

            double exchangeRate = rate.get();

            System.out.printf(
                    "%nCurrency conversion rate: %.2f%n%n",
                    exchangeRate
            );

            printResult("JFK", jfk.get(), exchangeRate);
            printResult("LAX", lax.get(), exchangeRate);
            printResult("LHR", lhr.get(), exchangeRate);
            printResult("XYZ", xyz.get(), exchangeRate);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Flight-price workflow interrupted.");

        } catch (ExecutionException e) {
            System.err.println(
                    "Asynchronous task failed: " + e.getCause()
            );

        } finally {
            shutdownExecutor(executor);
        }
    }

    private static void printResult(
            String code,
            double basePrice,
            double rate
    ) {
        System.out.printf(
                "%s Base Price: $%.2f USD | Total: €%.2f EUR%n",
                code,
                basePrice,
                basePrice * rate
        );
    }

    private static void shutdownExecutor(
            ExecutorService executor
    ) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}