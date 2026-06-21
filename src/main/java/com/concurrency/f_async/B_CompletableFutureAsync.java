package com.concurrency.f_async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class FlightPriceAnalyzerNonBlocking {
    private final ExecutorService executor;

    public FlightPriceAnalyzerNonBlocking(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Natively returns a CompletableFuture using supplyAsync.
     * This is non-blocking and enables functional chaining.
     */
    public CompletableFuture<Double> fetchPriceAsync(String airportCode, long simulateLatencyMs) {
        return CompletableFuture.supplyAsync(() -> {
            if (airportCode == null || airportCode.isBlank()) {
                throw new IllegalArgumentException("Invalid airport code provided.");
            }

            // Simulate network processing latency
            if (simulateLatencyMs > 0) {
                try {
                    Thread.sleep(simulateLatencyMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            System.out.format("[%s] Fetching base flight price for %s...\n", Thread.currentThread().getName(), airportCode);
            return switch (airportCode.toUpperCase()) {
                case "JFK" -> 450.00;
                case "LAX" -> 550.00;
                case "LHR" -> 720.00;
                default -> 300.00;
            };
        }, executor); // Pass our custom pool
    }
}

class ExchangeRateProviderNonBlocking {
    private final ExecutorService executor;

    public ExchangeRateProviderNonBlocking(ExecutorService executor) {
        this.executor = executor;
    }

    public CompletableFuture<Double> fetchExchangeRateAsync(String targetCurrency, long latencyMs) {
        return CompletableFuture.supplyAsync(() -> {
            System.out.format("[%s] Fetching exchange rate for %s...\n", Thread.currentThread().getName(), targetCurrency);
            if (latencyMs > 0) {
                try {
                    Thread.sleep(latencyMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return "EUR".equalsIgnoreCase(targetCurrency) ? 0.92 : 1.0;
        }, executor);
    }
}

public class B_CompletableFutureAsync {
    public static void main(String[] args) throws Exception {
        ExecutorService flightPricePool = Executors.newFixedThreadPool(8);

        try {
            FlightPriceAnalyzerNonBlocking analyzer = new FlightPriceAnalyzerNonBlocking(flightPricePool);
            ExchangeRateProviderNonBlocking rateProvider = new ExchangeRateProviderNonBlocking(flightPricePool);

            // 1. Start fetching the exchange rate dependency asynchronously
            CompletableFuture<Double> rateFuture = rateProvider.fetchExchangeRateAsync("EUR", 1000);

            System.out.println("\n--- Aggregated Final Conversion Matrix (CompletableFuture Pipeline) ---");

            // 2. Combine the flight price task and exchange rate task fluidly!
            // .thenCombine() takes another asynchronous stage, waits for BOTH to finish completely,
            // and then merges their results down into a final execution operation.
            CompletableFuture<Void> jfkPipeline = analyzer.fetchPriceAsync("JFK", 2000)
                    .thenCombine(rateFuture, (price, rate) -> printResult("JFK", price, rate));

            CompletableFuture<Void> laxPipeline = analyzer.fetchPriceAsync("LAX", 3000)
                    .thenCombine(rateFuture, (price, rate) -> printResult("LAX", price, rate));

            CompletableFuture<Void> lhrPipeline = analyzer.fetchPriceAsync("LHR", 1000)
                    .thenCombine(rateFuture, (price, rate) -> printResult("LHR", price, rate));

            CompletableFuture<Void> xyzPipeline = analyzer.fetchPriceAsync("XYZ", 1500)
                    .thenCombine(rateFuture, (price, rate) -> printResult("XYZ", price, rate));

            // 3. Tracking Barrier: Combine all pipelines into a single master gatekeeper object.
            // CompletableFuture.allOf() creates a new composite future that resolves only when every
            // single underlying task passed to it has finished its processing work.
            CompletableFuture<Void> allTasksGatekeeper = CompletableFuture.allOf(
                    jfkPipeline, laxPipeline, lhrPipeline, xyzPipeline
            );

            // This is the only place we block the main thread! We cleanly await global completion.
            // No loops, no volatile conditions, no thread pool starvation traps.
            allTasksGatekeeper.join();

            System.out.println("\n[Main Thread] All asynchronous processing channels closed successfully.");

        } finally {
            flightPricePool.shutdown();
        }
    }

    // Pure functional printing block. No catching checked exceptions or calling .get() required!
    private static Void printResult(String code, double basePrice, double rate) {
        System.out.format("[%s] %s Base Price: $%.2f USD | Total: €%.2f EUR\n",
                Thread.currentThread().getName(), code, basePrice, (basePrice * rate));
        return null;
    }
}