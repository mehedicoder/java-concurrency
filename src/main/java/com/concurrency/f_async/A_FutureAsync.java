package com.concurrency.f_async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class FlightPriceAnalyzer {

    // Injecting the executor allows us to swap thread pools between production and testing
    private final ExecutorService executor;

    public FlightPriceAnalyzer(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Submits a background task to fetch prices and returns a Future receipt instantly.
     */
    public Future<Double> fetchPriceAsync(String airportCode, long simulateLatencyMs) {
        return executor.submit(() -> {
            if (airportCode == null || airportCode.isBlank()) {
                throw new IllegalArgumentException("Invalid airport code provided.");
            }

            // Simulate network processing latency
            if (simulateLatencyMs > 0) {
                Thread.sleep(simulateLatencyMs);
            }

            // Simulate a dynamic mock pricing algorithm
            System.out.format("[%s] Fetching base flight price for the airport code %s...\n", Thread.currentThread().getName(), airportCode);
            return switch (airportCode.toUpperCase()) {
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

    public ExchangeRateProvider(ExecutorService executor) {
        this.executor = executor;
    }

    public Future<Double> fetchExchangeRateAsync(String targetCurrency, long latencyMs) {
        return executor.submit(() -> {
            System.out.format("[%s] 💱 Fetching exchange rate for %s...\n", Thread.currentThread().getName(), targetCurrency);
            if (latencyMs > 0) Thread.sleep(latencyMs);
            return "EUR".equalsIgnoreCase(targetCurrency) ? 0.92 : 1.0;
        });
    }
}

public class A_FutureAsync {
    public static void main(String[] args) throws Exception {
        // Using a slightly larger pool (or a CachedThreadPool) so printing tasks
        // don't have to wait for worker threads to die.
        ExecutorService flightPricePool = java.util.concurrent.Executors.newFixedThreadPool(8);

        try {
            FlightPriceAnalyzer analyzer = new FlightPriceAnalyzer(flightPricePool);
            ExchangeRateProvider rateProvider = new ExchangeRateProvider(flightPricePool);

            Future<Double> jfkPriceFuture = analyzer.fetchPriceAsync("JFK", 2000);
            Future<Double> laxPriceFuture = analyzer.fetchPriceAsync("LAX", 3000);
            Future<Double> lhrPriceFuture = analyzer.fetchPriceAsync("LHR", 1000);
            Future<Double> unknownPriceFuture = analyzer.fetchPriceAsync("XYZ", 1500);

            Future<Double> rateFuture = rateProvider.fetchExchangeRateAsync("EUR", 1000);

            double exchangeRate = rateFuture.get();
            System.out.format("\n[System Alert] Currency Conversion Rate Fixed: %.2f\n\n", exchangeRate);

            System.out.println("--- Aggregated Final Conversion Matrix (Async Processing) ---");

           // Submit the ENTIRE print execution block to your existing 8-thread pool.
           // This allows the main thread to instantly bypass these 4 lines without stalling!
            flightPricePool.submit(() -> printResultAsync("JFK", jfkPriceFuture, exchangeRate));
            flightPricePool.submit(() -> printResultAsync("LAX", laxPriceFuture, exchangeRate));
            flightPricePool.submit(() -> printResultAsync("LHR", lhrPriceFuture, exchangeRate));
            flightPricePool.submit(() -> printResultAsync("XYZ", unknownPriceFuture, exchangeRate));

            // Now your tracking loop actually has a job to do!
            while (!jfkPriceFuture.isDone() || !laxPriceFuture.isDone() || !lhrPriceFuture.isDone() || !unknownPriceFuture.isDone()) {
                Thread.sleep(100);
            }

        } finally {
            // graceful shutdown
            flightPricePool.shutdown();
        }
    }

    // Clean, Isolated Async Print Helper
    private static void printResultAsync(String code, Future<Double> future, double rate) {
        try {
            // This .get() runs inside a worker thread from flightPricePool.
            // It safely blocks THAT background thread, leaving the main thread perfectly free.
            double basePrice = future.get();
            System.out.format("[%s] %s Base Price: $%.2f USD | Total: €%.2f EUR\n",
                    Thread.currentThread().getName(), code, basePrice, (basePrice * rate));
        } catch (Exception e) {
            System.err.println("Failed to fetch price for " + code);
        }
    }
}
