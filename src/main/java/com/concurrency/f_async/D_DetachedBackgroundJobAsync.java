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
 * Responsible only for creating production dependencies,
 * starting the background job, and shutting down resources.
 */
public class D_DetachedBackgroundJobAsync {

    public static void main(String[] args) {
        ExecutorService backgroundJobPool = Executors.newFixedThreadPool(2);

        try {
            ReportExportService exportService = new SimulatedPdfExportService(Duration.ofMillis(2_500));
            BackgroundReportJob reportJob = new BackgroundReportJob(backgroundJobPool, exportService, System.out::println);

            System.out.println("[HTTP Thread] POST /api/reports/trigger-export received.");

            // 1. Fire the task completely in the background
            CompletableFuture<ExportResult> exportFuture = reportJob.triggerPdfExport("ANNUAL_TAX_REPORT_2026");

            // 2. Attach a detached callback for success/failure handling
            exportFuture.whenComplete((result, exception) -> {
                if (exception != null) {
                    System.err.println("Background report export failed: " + rootCause(exception).getMessage());
                } else {
                    System.out.printf("[Async Callback] Export completed successfully: %s%n", result.storageLocation());
                }
            });

            System.out.println("[HTTP Thread] Instantly returning Response: HTTP 202 (Accepted).");
            System.out.println("[HTTP Thread] Main thread execution completed. Turning off lights...");

        } finally {
            // 3. Initiate graceful shutdown.
            // The pool will stop accepting new tasks, but WILL finish running the active export task in the background.
            shutdownExecutor(backgroundJobPool);
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
                            "Background-job executor did not terminate."
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
 * Coordinates background report exports.
 * This class does not create its own executor, sleep directly,
 * or write directly to System.out.
 */
final class BackgroundReportJob {

    private final ExecutorService executor;
    private final ReportExportService exportService;
    private final Consumer<String> logger;

    BackgroundReportJob(
            ExecutorService executor,
            ReportExportService exportService,
            Consumer<String> logger
    ) {
        this.executor = Objects.requireNonNull(
                executor,
                "executor must not be null"
        );

        this.exportService = Objects.requireNonNull(
                exportService,
                "exportService must not be null"
        );

        this.logger = Objects.requireNonNull(
                logger,
                "logger must not be null"
        );
    }

    /**
     * Starts the export and returns immediately with a Future
     * that represents the background job.
     */
    CompletableFuture<ExportResult> triggerPdfExport(
            String reportName
    ) {
        validateReportName(reportName);

        return CompletableFuture.supplyAsync(
                () -> {
                    return performExport(reportName);
                },
                executor
        );
    }

    private ExportResult performExport(String reportName) {
        logger.accept(
                threadPrefix()
                        + "Beginning heavy compilation matrix for: "
                        + reportName
        );

        ExportResult result =
                exportService.export(reportName);

        logger.accept(
                threadPrefix()
                        + "Report finalized and uploaded to: "
                        + result.storageLocation()
        );

        return result;
    }

    private static void validateReportName(
            String reportName
    ) {
        if (reportName == null || reportName.isBlank()) {
            throw new IllegalArgumentException(
                    "Report name is required."
            );
        }
    }

    private static String threadPrefix() {
        return "["
                + Thread.currentThread().getName()
                + "] ";
    }
}

/**
 * Represents the external or expensive report-generation operation.
 * Tests can replace this interface with a fast lambda.
 */
@FunctionalInterface
interface ReportExportService {

    ExportResult export(String reportName);
}

/**
 * Immutable result returned when an export succeeds.
 */
record ExportResult(
        String reportName,
        String storageLocation
) {
    ExportResult {
        Objects.requireNonNull(
                reportName,
                "reportName must not be null"
        );

        Objects.requireNonNull(
                storageLocation,
                "storageLocation must not be null"
        );
    }
}

/**
 * Production-style simulation.
 * Unit tests do not need to use this implementation.
 */
final class SimulatedPdfExportService
        implements ReportExportService {

    private final Duration processingDelay;

    SimulatedPdfExportService(Duration processingDelay) {
        Objects.requireNonNull(
                processingDelay,
                "processingDelay must not be null"
        );

        if (processingDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "Processing delay cannot be negative."
            );
        }

        this.processingDelay = processingDelay;
    }

    @Override
    public ExportResult export(String reportName) {
        sleep(processingDelay);

        return new ExportResult(
                reportName,
                "s3://secure-reports/"
                        + reportName
                        + ".pdf"
        );
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }

        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new CompletionException(
                    "Report export was interrupted.",
                    e
            );
        }
    }
}