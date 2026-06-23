package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class CompletableFutureAsyncTest {

    private ExecutorService executor;
    private FlightPriceAnalyzerNonBlocking analyzer;
    private ExchangeRateProviderNonBlocking rateProvider;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(5);

        analyzer =
                new FlightPriceAnalyzerNonBlocking(executor);

        rateProvider =
                new ExchangeRateProviderNonBlocking(executor);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();

        try {
            assertTrue(
                    executor.awaitTermination(2, TimeUnit.SECONDS),
                    "The executor failed to terminate after the test."
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(
                    "The test thread was interrupted during executor shutdown.",
                    e
            );
        }
    }

    @Test
    @DisplayName(
            "Verify that a valid airport request completes asynchronously with the expected price"
    )
    void testFetchJfkPriceSuccessfully() {
        // Arrange
        String airportCode = "JFK";

        // Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync(airportCode, 200);

        // Assert
        assertNotNull(priceFuture);

        await()
                .atMost(Duration.ofSeconds(2))
                .until(priceFuture::isDone);

        assertAll(
                () -> assertFalse(priceFuture.isCancelled()),
                () -> assertFalse(
                        priceFuture.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        450.00,
                        priceFuture.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that airport codes are normalized before price resolution"
    )
    void testAirportCodeIsNormalized() {
        // Arrange & Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("jfk", 0);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        assertEquals(
                450.00,
                priceFuture.join(),
                0.001
        );
    }

    @Test
    @DisplayName(
            "Verify that an unknown airport receives the default flight price"
    )
    void testUnknownAirportReturnsDefaultPrice() {
        // Arrange & Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("XYZ", 100);

        await()
                .atMost(Duration.ofSeconds(2))
                .until(priceFuture::isDone);

        // Assert
        assertEquals(
                300.00,
                priceFuture.join(),
                0.001
        );
    }

    @Test
    @DisplayName(
            "Verify successful asynchronous retrieval of the EUR exchange rate"
    )
    void testFetchEurExchangeRateSuccessfully() {
        // Arrange & Act
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        100
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(rateFuture::isDone);

        // Assert
        assertAll(
                () -> assertFalse(
                        rateFuture.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        0.92,
                        rateFuture.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that an unsupported currency receives the fallback exchange rate"
    )
    void testUnsupportedCurrencyReturnsFallbackRate() {
        // Arrange & Act
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "GBP",
                        50
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(rateFuture::isDone);

        // Assert
        assertEquals(
                1.0,
                rateFuture.join(),
                0.001
        );
    }

    @Test
    @DisplayName(
            "Verify that a price and exchange rate are combined without blocking worker threads"
    )
    void testPriceAndExchangeRatePipelineSuccessfully() {
        // Arrange
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("JFK", 200);

        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        100
                );

        // Act
        CompletableFuture<Double> convertedPriceFuture =
                priceFuture.thenCombine(
                        rateFuture,
                        (price, rate) -> price * rate
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(convertedPriceFuture::isDone);

        // Assert
        assertAll(
                () -> assertFalse(
                        convertedPriceFuture
                                .isCompletedExceptionally()
                ),
                () -> assertEquals(
                        414.00,
                        convertedPriceFuture.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that thenAcceptBoth consumes the flight price and exchange rate"
    )
    void testThenAcceptBothConsumesBothResults() {
        // Arrange
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("LAX", 150);

        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        100
                );

        AtomicReference<Double> convertedPrice =
                new AtomicReference<>();

        // Act
        CompletableFuture<Void> pipeline =
                priceFuture.thenAcceptBoth(
                        rateFuture,
                        (price, rate) ->
                                convertedPrice.set(price * rate)
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(pipeline::isDone);

        // Assert
        assertAll(
                () -> assertFalse(
                        pipeline.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        506.00,
                        convertedPrice.get(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that allOf completes after all airport pipelines complete"
    )
    void testAllOfCompletesAfterAllPipelines() {
        // Arrange
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        100
                );

        CompletableFuture<Double> jfkPipeline =
                analyzer.fetchPriceAsync("JFK", 300)
                        .thenCombine(
                                rateFuture,
                                (price, rate) -> price * rate
                        );

        CompletableFuture<Double> laxPipeline =
                analyzer.fetchPriceAsync("LAX", 400)
                        .thenCombine(
                                rateFuture,
                                (price, rate) -> price * rate
                        );

        CompletableFuture<Double> lhrPipeline =
                analyzer.fetchPriceAsync("LHR", 200)
                        .thenCombine(
                                rateFuture,
                                (price, rate) -> price * rate
                        );

        CompletableFuture<Double> xyzPipeline =
                analyzer.fetchPriceAsync("XYZ", 250)
                        .thenCombine(
                                rateFuture,
                                (price, rate) -> price * rate
                        );

        // Act
        CompletableFuture<Void> allTasks =
                CompletableFuture.allOf(
                        jfkPipeline,
                        laxPipeline,
                        lhrPipeline,
                        xyzPipeline
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(allTasks::isDone);

        // Assert
        assertAll(
                () -> assertFalse(
                        allTasks.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        414.00,
                        jfkPipeline.join(),
                        0.001
                ),
                () -> assertEquals(
                        506.00,
                        laxPipeline.join(),
                        0.001
                ),
                () -> assertEquals(
                        662.40,
                        lhrPipeline.join(),
                        0.001
                ),
                () -> assertEquals(
                        276.00,
                        xyzPipeline.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a blank airport code completes the CompletableFuture exceptionally"
    )
    void testBlankAirportCodeCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("   ", 100);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                priceFuture::join
        );

        assertAll(
                () -> assertTrue(
                        priceFuture.isCompletedExceptionally()
                ),
                () -> assertFalse(priceFuture.isCancelled()),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        exception.getCause()
                ),
                () -> assertEquals(
                        "Invalid airport code provided.",
                        exception.getCause().getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a null airport code completes the CompletableFuture exceptionally"
    )
    void testNullAirportCodeCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync(null, 100);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                priceFuture::join
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                exception.getCause()
        );

        assertEquals(
                "Invalid airport code provided.",
                exception.getCause().getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that negative flight latency completes the task exceptionally"
    )
    void testNegativeFlightLatencyCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync("JFK", -1);

        await()
                .atMost(Duration.ofSeconds(1))
                .until(priceFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                priceFuture::join
        );

        assertAll(
                () -> assertTrue(
                        priceFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        exception.getCause()
                ),
                () -> assertEquals(
                        "Latency cannot be negative.",
                        exception.getCause().getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a blank currency completes the exchange-rate task exceptionally"
    )
    void testBlankCurrencyCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        " ",
                        100
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(rateFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                rateFuture::join
        );

        assertAll(
                () -> assertTrue(
                        rateFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        exception.getCause()
                ),
                () -> assertEquals(
                        "Target currency is required.",
                        exception.getCause().getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a null currency completes the exchange-rate task exceptionally"
    )
    void testNullCurrencyCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        null,
                        100
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(rateFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                rateFuture::join
        );

        assertInstanceOf(
                IllegalArgumentException.class,
                exception.getCause()
        );

        assertEquals(
                "Target currency is required.",
                exception.getCause().getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that negative exchange-rate latency completes the task exceptionally"
    )
    void testNegativeExchangeRateLatencyCompletesExceptionally() {
        // Arrange & Act
        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        -1
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(rateFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                rateFuture::join
        );

        assertAll(
                () -> assertTrue(
                        rateFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        exception.getCause()
                ),
                () -> assertEquals(
                        "Latency cannot be negative.",
                        exception.getCause().getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a failed flight-price stage prevents its dependent combination stage"
    )
    void testFailedPriceStagePreventsCombination() {
        // Arrange
        CompletableFuture<Double> failedPriceFuture =
                analyzer.fetchPriceAsync(" ", 100);

        CompletableFuture<Double> rateFuture =
                rateProvider.fetchExchangeRateAsync(
                        "EUR",
                        100
                );

        // Act
        CompletableFuture<Double> convertedPriceFuture =
                failedPriceFuture.thenCombine(
                        rateFuture,
                        (price, rate) -> price * rate
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(convertedPriceFuture::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                convertedPriceFuture::join
        );

        assertAll(
                () -> assertTrue(
                        convertedPriceFuture
                                .isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        rootCause(exception)
                ),
                () -> assertEquals(
                        "Invalid airport code provided.",
                        rootCause(exception).getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a failed shared exchange-rate Future causes every dependent pipeline to fail"
    )
    void testFailedSharedRateFutureFailsAllPipelines() {
        // Arrange
        CompletableFuture<Double> failedRateFuture =
                rateProvider.fetchExchangeRateAsync(
                        " ",
                        100
                );

        CompletableFuture<Double> jfkPipeline =
                analyzer.fetchPriceAsync("JFK", 100)
                        .thenCombine(
                                failedRateFuture,
                                (price, rate) -> price * rate
                        );

        CompletableFuture<Double> laxPipeline =
                analyzer.fetchPriceAsync("LAX", 100)
                        .thenCombine(
                                failedRateFuture,
                                (price, rate) -> price * rate
                        );

        // Act
        CompletableFuture<Void> allTasks =
                CompletableFuture.allOf(
                        jfkPipeline,
                        laxPipeline
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(allTasks::isDone);

        // Assert
        assertAll(
                () -> assertTrue(
                        failedRateFuture
                                .isCompletedExceptionally()
                ),
                () -> assertTrue(
                        jfkPipeline.isCompletedExceptionally()
                ),
                () -> assertTrue(
                        laxPipeline.isCompletedExceptionally()
                ),
                () -> assertTrue(
                        allTasks.isCompletedExceptionally()
                ),
                () -> assertThrows(
                        CompletionException.class,
                        allTasks::join
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that allOf completes exceptionally when one underlying task fails"
    )
    void testAllOfCompletesExceptionallyWhenOneTaskFails() {
        // Arrange
        CompletableFuture<Double> successfulFuture =
                analyzer.fetchPriceAsync("JFK", 100);

        CompletableFuture<Double> failedFuture =
                analyzer.fetchPriceAsync(" ", 100);

        // Act
        CompletableFuture<Void> allTasks =
                CompletableFuture.allOf(
                        successfulFuture,
                        failedFuture
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(allTasks::isDone);

        // Assert
        CompletionException exception = assertThrows(
                CompletionException.class,
                allTasks::join
        );

        assertAll(
                () -> assertTrue(successfulFuture.isDone()),
                () -> assertFalse(
                        successfulFuture
                                .isCompletedExceptionally()
                ),
                () -> assertTrue(
                        failedFuture.isCompletedExceptionally()
                ),
                () -> assertTrue(
                        allTasks.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        rootCause(exception)
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that exceptionally can recover from a failed price lookup"
    )
    void testExceptionallyRecoversFromFailure() {
        // Arrange
        CompletableFuture<Double> failedPriceFuture =
                analyzer.fetchPriceAsync(" ", 100);

        // Act
        CompletableFuture<Double> recoveredFuture =
                failedPriceFuture.exceptionally(
                        error -> 300.00
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(recoveredFuture::isDone);

        // Assert
        assertAll(
                () -> assertTrue(
                        failedPriceFuture
                                .isCompletedExceptionally()
                ),
                () -> assertFalse(
                        recoveredFuture
                                .isCompletedExceptionally()
                ),
                () -> assertEquals(
                        300.00,
                        recoveredFuture.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that handle can transform both successful and failed outcomes"
    )
    void testHandleTransformsFailedOutcome() {
        // Arrange
        CompletableFuture<Double> failedPriceFuture =
                analyzer.fetchPriceAsync(" ", 100);

        // Act
        CompletableFuture<Double> handledFuture =
                failedPriceFuture.handle(
                        (price, error) ->
                                error == null ? price : 0.00
                );

        await()
                .atMost(Duration.ofSeconds(1))
                .until(handledFuture::isDone);

        // Assert
        assertAll(
                () -> assertTrue(
                        failedPriceFuture
                                .isCompletedExceptionally()
                ),
                () -> assertFalse(
                        handledFuture
                                .isCompletedExceptionally()
                ),
                () -> assertEquals(
                        0.00,
                        handledFuture.join(),
                        0.001
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that cancelling a CompletableFuture changes its state to cancelled"
    )
    void testCancelCompletableFuture() {
        // Arrange
        CompletableFuture<Double> priceFuture =
                analyzer.fetchPriceAsync(
                        "JFK",
                        5_000
                );

        // Act
        boolean cancellationAccepted =
                priceFuture.cancel(true);

        // Assert
        assertAll(
                () -> assertTrue(cancellationAccepted),
                () -> assertTrue(priceFuture.isDone()),
                () -> assertTrue(priceFuture.isCancelled()),
                () -> assertTrue(
                        priceFuture.isCompletedExceptionally()
                ),
                () -> assertThrows(
                        java.util.concurrent.CancellationException.class,
                        priceFuture::join
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that submitting work after executor shutdown is immediately rejected"
    )
    void testSubmissionAfterExecutorShutdownIsRejected() {
        // Arrange
        executor.shutdown();

        // Act & Assert
        assertThrows(
                RejectedExecutionException.class,
                () -> analyzer.fetchPriceAsync(
                        "JFK",
                        100
                )
        );
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }
}
