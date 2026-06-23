package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class FutureAsyncTest {

    private ExecutorService executor;
    private FlightPriceAnalyzer analyzer;
    private ExchangeRateProvider rateProvider;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(5);
        analyzer = new FlightPriceAnalyzer(executor);
        rateProvider = new ExchangeRateProvider(executor);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();

        assertTrue(
                executor.awaitTermination(2, TimeUnit.SECONDS),
                "The executor failed to terminate after the test."
        );
    }

    @Test
    @DisplayName("Verify that a valid airport request completes asynchronously with the expected price")
    void testFetchJfkPriceSuccessfully() throws Exception {
        // Arrange
        String airportCode = "JFK";
        long simulatedLatency = 200;

        // Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync(airportCode, simulatedLatency);

        // Assert: the call returns immediately with an incomplete Future
        assertNotNull(priceFuture);
        assertFalse(
                priceFuture.isDone(),
                "The asynchronous task unexpectedly completed immediately."
        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(priceFuture::isDone);

        assertFalse(priceFuture.isCancelled());
        assertEquals(450.00, priceFuture.get());
    }

    @Test
    @DisplayName("Verify that airport codes are normalized before price resolution")
    void testAirportCodeIsNormalized() throws Exception {
        // Arrange
        String lowercaseAirportCode = "jfk";

        // Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync(lowercaseAirportCode, 0);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        assertEquals(450.00, priceFuture.get());
    }

    @Test
    @DisplayName("Verify that an unknown airport receives the default flight price")
    void testUnknownAirportReturnsDefaultPrice() throws Exception {
        // Arrange
        String unknownAirportCode = "XYZ";

        // Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync(unknownAirportCode, 100);

        await()
                .atMost(Duration.ofSeconds(2))
                .until(priceFuture::isDone);

        // Assert
        assertEquals(300.00, priceFuture.get());
    }

    @Test
    @DisplayName("Verify that multiple flight-price requests execute concurrently")
    void testMultipleAirportRequestsExecuteLikeAsynchronously() throws Exception {
        // Arrange & Act
        Future<Double> jfkFuture =
                analyzer.fetchPriceAsync("JFK", 300);

        Future<Double> laxFuture =
                analyzer.fetchPriceAsync("LAX", 400);

        Future<Double> lhrFuture =
                analyzer.fetchPriceAsync("LHR", 200);

        Future<Double> unknownFuture =
                analyzer.fetchPriceAsync("XYZ", 250);

        // Assert: Awaitility waits until all asynchronous tasks complete
        await()
                .atMost(Duration.ofSeconds(2))
                .until(() ->
                        jfkFuture.isDone()
                                && laxFuture.isDone()
                                && lhrFuture.isDone()
                                && unknownFuture.isDone()
                );

        assertAll(
                () -> assertEquals(450.00, jfkFuture.get()),
                () -> assertEquals(550.00, laxFuture.get()),
                () -> assertEquals(720.00, lhrFuture.get()),
                () -> assertEquals(300.00, unknownFuture.get())
        );
    }

    @Test
    @DisplayName("Verify that multiple flight-price requests execute concurrently")
    void testMultipleAirportRequestsActuallyExecuteConcurrently() throws Exception {
        // Arrange
        long start = System.nanoTime();

        // Act
        Future<Double> jfkFuture =
                analyzer.fetchPriceAsync("JFK", 300);

        Future<Double> laxFuture =
                analyzer.fetchPriceAsync("LAX", 400);

        Future<Double> lhrFuture =
                analyzer.fetchPriceAsync("LHR", 200);

        Future<Double> unknownFuture =
                analyzer.fetchPriceAsync("XYZ", 250);

        await()
                .atMost(Duration.ofSeconds(2))
                .until(() ->
                        jfkFuture.isDone()
                                && laxFuture.isDone()
                                && lhrFuture.isDone()
                                && unknownFuture.isDone()
                );

        long elapsedMillis =
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // Assert
        assertAll(
                () -> assertEquals(450.00, jfkFuture.get()),
                () -> assertEquals(550.00, laxFuture.get()),
                () -> assertEquals(720.00, lhrFuture.get()),
                () -> assertEquals(300.00, unknownFuture.get()),
                () -> assertTrue(
                        elapsedMillis < 900,
                        "Tasks appear to have executed sequentially. Elapsed: "
                                + elapsedMillis + " ms"
                )
        );
    }

    @Test
    @DisplayName("Verify that a blank airport code completes the Future exceptionally")
    void testBlankAirportCodeCompletesExceptionally() {
        // Arrange
        String invalidAirportCode = "   ";

        // Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync(invalidAirportCode, 200);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert:
        // The worker throws IllegalArgumentException.
        // Future.get() exposes it inside ExecutionException.
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                priceFuture::get
        );

        Throwable originalCause = executionException.getCause();

        assertAll(
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        originalCause
                ),
                () -> assertEquals(
                        "Invalid airport code provided.",
                        originalCause.getMessage()
                ),
                () -> assertFalse(priceFuture.isCancelled())
        );
    }

    @Test
    @DisplayName("Verify that a null airport code is reported through ExecutionException")
    void testNullAirportCodeCompletesExceptionally() {
        // Arrange & Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync(null, 200);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                priceFuture::get
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                executionException.getCause()
        );

        assertEquals(
                "Invalid airport code provided.",
                executionException.getCause().getMessage()
        );
    }

    @Test
    @DisplayName("Verify that negative flight latency fails before network simulation begins")
    void testNegativeFlightLatencyCompletesExceptionally() {
        // Arrange & Act
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync("JFK", -1);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                priceFuture::get
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                executionException.getCause()
        );

        assertEquals(
                "Latency cannot be negative.",
                executionException.getCause().getMessage()
        );
    }

    @Test
    @DisplayName("Verify successful asynchronous retrieval of the EUR exchange rate")
    void testFetchEurExchangeRateSuccessfully() throws Exception {
        // Arrange
        String targetCurrency = "EUR";

        // Act
        Future<Double> exchangeRateFuture =
                rateProvider.fetchExchangeRateAsync(targetCurrency, 100);

        await()
                .atMost(Duration.ofSeconds(2))
                .until(exchangeRateFuture::isDone);

        // Assert
        assertEquals(0.92, exchangeRateFuture.get());
    }

    @Test
    @DisplayName("Verify that an unsupported currency receives the fallback exchange rate")
    void testUnsupportedCurrencyReturnsFallbackRate() throws Exception {
        // Arrange & Act
        Future<Double> exchangeRateFuture =
                rateProvider.fetchExchangeRateAsync("GBP", 50);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(exchangeRateFuture::isDone);

        // Assert
        assertEquals(1.0, exchangeRateFuture.get());
    }

    @Test
    @DisplayName("Verify that a blank currency completes the exchange-rate task exceptionally")
    void testBlankCurrencyCompletesExceptionally() {
        // Arrange & Act
        Future<Double> exchangeRateFuture =
                rateProvider.fetchExchangeRateAsync(" ", 100);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(exchangeRateFuture::isDone);

        // Assert
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                exchangeRateFuture::get
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                executionException.getCause()
        );

        assertEquals(
                "Target currency is required.",
                executionException.getCause().getMessage()
        );
    }

    @Test
    @DisplayName("Verify that negative exchange-rate latency completes the task exceptionally")
    void testNegativeExchangeRateLatencyCompletesExceptionally() {
        // Arrange & Act
        Future<Double> exchangeRateFuture =
                rateProvider.fetchExchangeRateAsync("EUR", -10);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(exchangeRateFuture::isDone);

        // Assert
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                exchangeRateFuture::get
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                executionException.getCause()
        );

        assertEquals(
                "Latency cannot be negative.",
                executionException.getCause().getMessage()
        );
    }

    @Test
    @DisplayName("Verify that cancelling a sleeping flight task interrupts and cancels its Future")
    void testCancelLongRunningPriceTask() {
        // Arrange
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync("JFK", 5_000);

        // Act
        boolean cancellationAccepted = priceFuture.cancel(true);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        assertTrue(
                cancellationAccepted,
                "The executor refused to cancel the running task."
        );

        assertTrue(priceFuture.isCancelled());

        assertThrows(
                CancellationException.class,
                priceFuture::get
        );
    }

    @Test
    @DisplayName("Verify that submitting work after executor shutdown is immediately rejected")
    void testSubmissionAfterExecutorShutdownIsRejected() {
        // Arrange
        executor.shutdown();

        // Act & Assert:
        // This exception is thrown immediately by submit().
        // No Future is returned.
        assertThrows(
                RejectedExecutionException.class,
                () -> analyzer.fetchPriceAsync("JFK", 100)
        );
    }

    @Test
    @DisplayName("Verify that graceful shutdown allows a pending task to finish")
    void testExecutorTerminatesAfterPendingTaskCompletes() throws Exception {
        // Arrange
        Future<Double> priceFuture =
                analyzer.fetchPriceAsync("JFK", 100);

        // Act
        executor.shutdown();

        boolean terminated =
                executor.awaitTermination(2, TimeUnit.SECONDS);

        // Assert
        assertAll(
                () -> assertTrue(terminated),
                () -> assertTrue(executor.isShutdown()),
                () -> assertTrue(executor.isTerminated()),
                () -> assertTrue(priceFuture.isDone()),
                () -> assertFalse(priceFuture.isCancelled()),
                () -> assertEquals(450.00, priceFuture.get())
        );
    }
}
