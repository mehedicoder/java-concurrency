package com.concurrency.f_async;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Application entry point.
 * This class is responsible only for wiring production dependencies
 * and starting the workflow.
 */
public class C_AdvancedCompletableFuture {

    public static void main(String[] args) {
        ExecutorService logisticsPool =
                Executors.newFixedThreadPool(4);

        try {
            LogisticsWorkflow workflow =
                    new LogisticsWorkflow(
                            logisticsPool,
                            new SimulatedWeightValidationService(
                                    Duration.ofMillis(1_500)
                            ),
                            new SimulatedCustomsClearanceService(
                                    Duration.ofSeconds(6)
                            ),
                            new SimulatedManifestService(
                                    Duration.ofSeconds(1)
                            ),
                            Duration.ofSeconds(4),
                            System.out::println
                    );

            System.out.printf(
                    "[%s] Initializing Logistics Distribution Control...%n",
                    Thread.currentThread().getName()
            );

            CompletableFuture<LogisticsResult> resultFuture =
                    workflow.processShipment("SHIPMENT-1001");

            System.out.println(
                    "\n[Main Thread] Ground operations are idling. "
                            + "Awaiting dependent clearance pipelines...\n"
            );

            LogisticsResult result = resultFuture.join();

            System.out.printf(
                    "%n[%s] Global Processing Complete. "
                            + "System Result Token: %s%n",
                    Thread.currentThread().getName(),
                    result.manifestStatus()
            );

        } catch (CompletionException e) {
            Throwable cause = rootCause(e);

            System.err.println(
                    "Logistics processing failed: "
                            + cause.getMessage()
            );

        } finally {
            shutdownExecutor(logisticsPool);
        }
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
                            "Logistics executor did not terminate."
                    );
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

/**
 * Coordinates the asynchronous business workflow.
 */
final class LogisticsWorkflow {

    static final String DOMESTIC_REROUTE_TOKEN =
            "DOMESTIC-RE-ROUTE";

    private final ExecutorService executor;
    private final WeightValidationService weightValidationService;
    private final CustomsClearanceService customsClearanceService;
    private final ManifestService manifestService;
    private final Duration customsTimeout;
    private final Consumer<String> logger;

    LogisticsWorkflow(
            ExecutorService executor,
            WeightValidationService weightValidationService,
            CustomsClearanceService customsClearanceService,
            ManifestService manifestService,
            Duration customsTimeout,
            Consumer<String> logger
    ) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.weightValidationService = Objects.requireNonNull(
                weightValidationService,
                "weightValidationService must not be null"
        );

        this.customsClearanceService = Objects.requireNonNull(
                customsClearanceService,
                "customsClearanceService must not be null"
        );

        this.manifestService = Objects.requireNonNull(
                manifestService,
                "manifestService must not be null"
        );

        this.customsTimeout = requirePositiveDuration(
                customsTimeout
        );

        this.logger = Objects.requireNonNull(
                logger,
                "logger must not be null"
        );
    }

    CompletableFuture<LogisticsResult> processShipment(
            String shipmentId
    ) {
        validateShipmentId(shipmentId);

        CompletableFuture<Boolean> weightValidationFuture =
                CompletableFuture.supplyAsync(
                        () -> validateWeight(shipmentId),
                        executor
                );

        CompletableFuture<String> clearanceFuture =
                CompletableFuture.supplyAsync(
                                () -> requestCustomsClearance(
                                        shipmentId
                                ),
                                executor
                        )
                        .orTimeout(
                                customsTimeout.toMillis(),
                                TimeUnit.MILLISECONDS
                        )
                        .exceptionally(
                                error -> recoverFromCustomsFailure(
                                        shipmentId,
                                        error
                                )
                        );

        CompletableFuture<String> manifestFuture =
                clearanceFuture.thenCompose(
                        clearanceToken ->
                                CompletableFuture.supplyAsync(
                                        () -> generateManifest(
                                                shipmentId,
                                                clearanceToken
                                        ),
                                        executor
                                )
                );

        return weightValidationFuture.thenCombine(
                manifestFuture,
                (weightValid, manifestStatus) ->
                        new LogisticsResult(
                                shipmentId,
                                weightValid,
                                manifestStatus
                        )
        );
    }

    private boolean validateWeight(String shipmentId) {
        logger.accept(
                threadPrefix()
                        + "[Step 1] Scanning cargo mass "
                        + "and distribution indices..."
        );

        boolean valid =
                weightValidationService.validate(shipmentId);

        if (!valid) {
            throw new WeightValidationException(
                    "Shipment weight validation failed: "
                            + shipmentId
            );
        }

        logger.accept(
                threadPrefix()
                        + "[Step 1 Completed] Weight metrics "
                        + "within safe thresholds."
        );

        logger.accept(
                "[Log] Ground crew preparing plane loading pads..."
        );

        return true;
    }

    private String requestCustomsClearance(
            String shipmentId
    ) {
        logger.accept(
                threadPrefix()
                        + "[Step 2] Transmitting cross-border "
                        + "customs paperwork..."
        );

        String clearanceToken =
                customsClearanceService.clear(shipmentId);

        if (clearanceToken == null
                || clearanceToken.isBlank()) {
            throw new CustomsClearanceException(
                    "Customs service returned an invalid token."
            );
        }

        logger.accept(
                threadPrefix()
                        + "[Step 2 Completed] International "
                        + "customs clearance approved."
        );

        return clearanceToken;
    }

    private String recoverFromCustomsFailure(
            String shipmentId,
            Throwable error
    ) {
        Throwable cause = unwrap(error);

        logger.accept(
                threadPrefix()
                        + "System Alert: Customs processing "
                        + "breached SLA or failed: "
                        + cause.getMessage()
        );

        logger.accept(
                threadPrefix()
                        + "Rerouting shipment "
                        + shipmentId
                        + " to domestic holding facilities..."
        );

        return DOMESTIC_REROUTE_TOKEN;
    }

    private String generateManifest(
            String shipmentId,
            String clearanceToken
    ) {
        logger.accept(
                threadPrefix()
                        + "[Step 3] Token ["
                        + clearanceToken
                        + "] verified. Generating flight "
                        + "departure manifest..."
        );

        return manifestService.generate(
                shipmentId,
                clearanceToken
        );
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;

        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }

    private static String threadPrefix() {
        return "[" + Thread.currentThread().getName() + "] ";
    }

    private static void validateShipmentId(
            String shipmentId
    ) {
        if (shipmentId == null || shipmentId.isBlank()) {
            throw new IllegalArgumentException(
                    "Shipment ID is required."
            );
        }
    }

    private static Duration requirePositiveDuration(
            Duration duration
    ) {
        Objects.requireNonNull(
                duration,
                "customsTimeout must not be null"
        );

        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(
                    "Customs timeout must be positive."
            );
        }

        return duration;
    }
}

record LogisticsResult(
        String shipmentId,
        boolean weightValid,
        String manifestStatus
) {
}

@FunctionalInterface
interface WeightValidationService {

    boolean validate(String shipmentId);
}

@FunctionalInterface
interface CustomsClearanceService {

    String clear(String shipmentId);
}

@FunctionalInterface
interface ManifestService {

    String generate(
            String shipmentId,
            String clearanceToken
    );
}

final class SimulatedWeightValidationService
        implements WeightValidationService {

    private final Duration delay;

    SimulatedWeightValidationService(Duration delay) {
        this.delay = Objects.requireNonNull(delay);
    }

    @Override
    public boolean validate(String shipmentId) {
        SleepSupport.sleep(delay);
        return true;
    }
}

final class SimulatedCustomsClearanceService
        implements CustomsClearanceService {

    private final Duration delay;

    SimulatedCustomsClearanceService(Duration delay) {
        this.delay = Objects.requireNonNull(delay);
    }

    @Override
    public String clear(String shipmentId) {
        SleepSupport.sleep(delay);
        return "AUTH-99281-XM";
    }
}

final class SimulatedManifestService
        implements ManifestService {

    private final Duration delay;

    SimulatedManifestService(Duration delay) {
        this.delay = Objects.requireNonNull(delay);
    }

    @Override
    public String generate(
            String shipmentId,
            String clearanceToken
    ) {
        SleepSupport.sleep(delay);

        return "MANIFEST-FINALIZED-DELIVERY-SUCCESS";
    }
}

final class SleepSupport {

    private SleepSupport() {
    }

    static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }

        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new CompletionException(
                    "Asynchronous logistics operation interrupted.",
                    e
            );
        }
    }
}

class WeightValidationException extends RuntimeException {

    WeightValidationException(String message) {
        super(message);
    }
}

class CustomsClearanceException extends RuntimeException {

    CustomsClearanceException(String message) {
        super(message);
    }
}
