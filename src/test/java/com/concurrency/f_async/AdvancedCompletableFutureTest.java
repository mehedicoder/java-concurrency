package com.concurrency.f_async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class AdvancedCompletableFutureTest {

    private ExecutorService executor;
    private List<String> logs;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
        logs = new CopyOnWriteArrayList<>();
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
            fail("Test thread was interrupted during executor shutdown.", e);
        }
    }

    @Test
    @DisplayName("Verify successful shipment processing across weight, customs and manifest stages")
    void testProcessShipmentSuccessfully() {
        // Arrange
        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService customsService =
                shipmentId -> "TEST-AUTH-TOKEN";

        ManifestService manifestService =
                (shipmentId, clearanceToken) ->
                        "MANIFEST-CREATED-" + clearanceToken;

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<LogisticsResult> resultFuture =
                workflow.processShipment("SHIPMENT-1001");

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        LogisticsResult result = resultFuture.join();

        // Assert
        assertAll(
                () -> assertFalse(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        "SHIPMENT-1001",
                        result.shipmentId()
                ),
                () -> assertTrue(result.weightValid()),
                () -> assertEquals(
                        "MANIFEST-CREATED-TEST-AUTH-TOKEN",
                        result.manifestStatus()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that the customs token is passed to the manifest service"
    )
    void testClearanceTokenIsPassedToManifestService() {
        // Arrange
        AtomicReference<String> receivedShipmentId =
                new AtomicReference<>();

        AtomicReference<String> receivedClearanceToken =
                new AtomicReference<>();

        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService customsService =
                shipmentId -> "AUTH-12345";

        ManifestService manifestService =
                (shipmentId, clearanceToken) -> {
                    receivedShipmentId.set(shipmentId);
                    receivedClearanceToken.set(clearanceToken);
                    return "MANIFEST-SUCCESS";
                };

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        // Act
        LogisticsResult result =
                workflow.processShipment("SHIPMENT-2001").join();

        // Assert
        assertAll(
                () -> assertEquals(
                        "SHIPMENT-2001",
                        receivedShipmentId.get()
                ),
                () -> assertEquals(
                        "AUTH-12345",
                        receivedClearanceToken.get()
                ),
                () -> assertEquals(
                        "MANIFEST-SUCCESS",
                        result.manifestStatus()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a customs exception falls back to the domestic reroute token"
    )
    void testCustomsFailureUsesDomesticRerouteFallback() {
        // Arrange
        AtomicReference<String> manifestToken =
                new AtomicReference<>();

        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService customsService =
                shipmentId -> {
                    throw new CustomsClearanceException(
                            "Customs gateway unavailable."
                    );
                };

        ManifestService manifestService =
                (shipmentId, clearanceToken) -> {
                    manifestToken.set(clearanceToken);
                    return "DOMESTIC-MANIFEST-CREATED";
                };

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        // Act
        LogisticsResult result =
                workflow.processShipment("SHIPMENT-3001").join();

        // Assert
        assertAll(
                () -> assertTrue(result.weightValid()),
                () -> assertEquals(
                        LogisticsWorkflow.DOMESTIC_REROUTE_TOKEN,
                        manifestToken.get()
                ),
                () -> assertEquals(
                        "DOMESTIC-MANIFEST-CREATED",
                        result.manifestStatus()
                ),
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Customs gateway unavailable."
                                        )
                        )
                ),
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Rerouting shipment"
                                        )
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a customs timeout falls back to domestic rerouting"
    )
    void testCustomsTimeoutUsesDomesticRerouteFallback() {
        // Arrange
        CountDownLatch releaseCustomsService =
                new CountDownLatch(1);

        AtomicReference<String> manifestToken =
                new AtomicReference<>();

        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService slowCustomsService =
                shipmentId -> {
                    try {
                        releaseCustomsService.await();
                        return "LATE-AUTH-TOKEN";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        ManifestService manifestService =
                (shipmentId, clearanceToken) -> {
                    manifestToken.set(clearanceToken);
                    return "TIMEOUT-FALLBACK-MANIFEST";
                };

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                slowCustomsService,
                manifestService,
                Duration.ofMillis(100)
        );

        try {
            // Act
            CompletableFuture<LogisticsResult> resultFuture =
                    workflow.processShipment("SHIPMENT-4001");

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(resultFuture::isDone);

            LogisticsResult result = resultFuture.join();

            // Assert
            assertAll(
                    () -> assertFalse(
                            resultFuture.isCompletedExceptionally()
                    ),
                    () -> assertEquals(
                            LogisticsWorkflow.DOMESTIC_REROUTE_TOKEN,
                            manifestToken.get()
                    ),
                    () -> assertEquals(
                            "TIMEOUT-FALLBACK-MANIFEST",
                            result.manifestStatus()
                    ),
                    () -> assertTrue(
                            logs.stream().anyMatch(
                                    message ->
                                            message.contains(
                                                    "breached SLA or failed"
                                            )
                            )
                    )
            );

        } finally {
            releaseCustomsService.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that failed weight validation completes the workflow exceptionally"
    )
    void testWeightValidationFailureCompletesWorkflowExceptionally() {
        // Arrange
        WeightValidationService rejectedWeightService =
                shipmentId -> false;

        CustomsClearanceService customsService =
                shipmentId -> "AUTH-VALID";

        ManifestService manifestService =
                (shipmentId, clearanceToken) ->
                        "MANIFEST-SHOULD-NOT-BECOME-FINAL-RESULT";

        LogisticsWorkflow workflow = createWorkflow(
                rejectedWeightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<LogisticsResult> resultFuture =
                workflow.processShipment("SHIPMENT-5001");

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        Throwable cause = rootCause(exception);

        assertAll(
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        WeightValidationException.class,
                        cause
                ),
                () -> assertEquals(
                        "Shipment weight validation failed: SHIPMENT-5001",
                        cause.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that manifest generation failure completes the workflow exceptionally"
    )
    void testManifestFailureCompletesWorkflowExceptionally() {
        // Arrange
        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService customsService =
                shipmentId -> "AUTH-VALID";

        ManifestService brokenManifestService =
                (shipmentId, clearanceToken) -> {
                    throw new IllegalStateException(
                            "Manifest database unavailable."
                    );
                };

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                customsService,
                brokenManifestService,
                Duration.ofSeconds(1)
        );

        // Act
        CompletableFuture<LogisticsResult> resultFuture =
                workflow.processShipment("SHIPMENT-6001");

        await()
                .atMost(Duration.ofSeconds(2))
                .until(resultFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                resultFuture::join
        );

        // Assert
        Throwable cause = rootCause(exception);

        assertAll(
                () -> assertTrue(
                        resultFuture.isCompletedExceptionally()
                ),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "Manifest database unavailable.",
                        cause.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that an invalid customs token activates the domestic fallback"
    )
    void testBlankCustomsTokenActivatesFallback() {
        // Arrange
        AtomicReference<String> receivedToken =
                new AtomicReference<>();

        WeightValidationService weightService =
                shipmentId -> true;

        CustomsClearanceService customsService =
                shipmentId -> "   ";

        ManifestService manifestService =
                (shipmentId, token) -> {
                    receivedToken.set(token);
                    return "FALLBACK-MANIFEST";
                };

        LogisticsWorkflow workflow = createWorkflow(
                weightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        // Act
        LogisticsResult result =
                workflow.processShipment("SHIPMENT-7001").join();

        // Assert
        assertAll(
                () -> assertEquals(
                        LogisticsWorkflow.DOMESTIC_REROUTE_TOKEN,
                        receivedToken.get()
                ),
                () -> assertEquals(
                        "FALLBACK-MANIFEST",
                        result.manifestStatus()
                ),
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "invalid token"
                                        )
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a blank shipment ID is rejected before asynchronous work starts"
    )
    void testBlankShipmentIdIsRejectedImmediately() {
        // Arrange
        LogisticsWorkflow workflow = createWorkflow(
                shipmentId -> true,
                shipmentId -> "AUTH-VALID",
                (shipmentId, token) -> "MANIFEST-VALID",
                Duration.ofSeconds(1)
        );

        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> workflow.processShipment("   ")
        );

        assertEquals(
                "Shipment ID is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that a null shipment ID is rejected before asynchronous work starts"
    )
    void testNullShipmentIdIsRejectedImmediately() {
        // Arrange
        LogisticsWorkflow workflow = createWorkflow(
                shipmentId -> true,
                shipmentId -> "AUTH-VALID",
                (shipmentId, token) -> "MANIFEST-VALID",
                Duration.ofSeconds(1)
        );

        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> workflow.processShipment(null)
        );

        assertEquals(
                "Shipment ID is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that a zero customs timeout is rejected during workflow construction"
    )
    void testZeroCustomsTimeoutIsRejected() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new LogisticsWorkflow(
                        executor,
                        shipmentId -> true,
                        shipmentId -> "AUTH-VALID",
                        (shipmentId, token) -> "MANIFEST-VALID",
                        Duration.ZERO,
                        logs::add
                )
        );

        assertEquals(
                "Customs timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that a negative customs timeout is rejected during workflow construction"
    )
    void testNegativeCustomsTimeoutIsRejected() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new LogisticsWorkflow(
                        executor,
                        shipmentId -> true,
                        shipmentId -> "AUTH-VALID",
                        (shipmentId, token) -> "MANIFEST-VALID",
                        Duration.ofMillis(-1),
                        logs::add
                )
        );

        assertEquals(
                "Customs timeout must be positive.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that the final result waits for both weight validation and manifest processing"
    )
    void testFinalResultWaitsForBothWorkflowBranches() {
        // Arrange
        CountDownLatch releaseWeightValidation =
                new CountDownLatch(1);

        WeightValidationService delayedWeightService =
                shipmentId -> {
                    try {
                        releaseWeightValidation.await();
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        CustomsClearanceService customsService =
                shipmentId -> "AUTH-VALID";

        ManifestService manifestService =
                (shipmentId, token) -> "MANIFEST-COMPLETE";

        LogisticsWorkflow workflow = createWorkflow(
                delayedWeightService,
                customsService,
                manifestService,
                Duration.ofSeconds(1)
        );

        try {
            // Act
            CompletableFuture<LogisticsResult> resultFuture =
                    workflow.processShipment("SHIPMENT-8001");

            await()
                    .during(Duration.ofMillis(100))
                    .atMost(Duration.ofMillis(300))
                    .until(() -> !resultFuture.isDone());

            releaseWeightValidation.countDown();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(resultFuture::isDone);

            // Assert
            LogisticsResult result = resultFuture.join();

            assertAll(
                    () -> assertTrue(result.weightValid()),
                    () -> assertEquals(
                            "MANIFEST-COMPLETE",
                            result.manifestStatus()
                    )
            );

        } finally {
            releaseWeightValidation.countDown();
        }
    }

    private LogisticsWorkflow createWorkflow(
            WeightValidationService weightService,
            CustomsClearanceService customsService,
            ManifestService manifestService,
            Duration timeout
    ) {
        return new LogisticsWorkflow(
                executor,
                weightService,
                customsService,
                manifestService,
                timeout,
                logs::add
        );
    }

    private static Throwable rootCause(
            Throwable throwable
    ) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }
}
