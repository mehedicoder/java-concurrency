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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class DetachedBackgroundJobAsyncTest {

    private ExecutorService executor;
    private List<String> logs;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(2);
        logs = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();

        try {
            assertTrue(
                    executor.awaitTermination(2, TimeUnit.SECONDS),
                    "The background-job executor failed to terminate."
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            fail(
                    "The test thread was interrupted while shutting down the executor.",
                    e
            );
        }
    }

    @Test
    @DisplayName(
            "Verify that a valid report export completes successfully"
    )
    void testReportExportCompletesSuccessfully() {
        // Arrange
        ReportExportService fakeExportService =
                reportName -> new ExportResult(
                        reportName,
                        "test://exports/" + reportName + ".pdf"
                );

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        // Act
        CompletableFuture<ExportResult> exportFuture =
                reportJob.triggerPdfExport(
                        "ANNUAL_TAX_REPORT_2026"
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(exportFuture::isDone);

        ExportResult result = exportFuture.join();

        // Assert
        assertAll(
                () -> assertFalse(
                        exportFuture.isCompletedExceptionally()
                ),
                () -> assertFalse(exportFuture.isCancelled()),
                () -> assertEquals(
                        "ANNUAL_TAX_REPORT_2026",
                        result.reportName()
                ),
                () -> assertEquals(
                        "test://exports/ANNUAL_TAX_REPORT_2026.pdf",
                        result.storageLocation()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that the report name is passed to the export service"
    )
    void testReportNameIsPassedToExportService() {
        // Arrange
        AtomicReference<String> receivedReportName =
                new AtomicReference<>();

        ReportExportService fakeExportService =
                reportName -> {
                    receivedReportName.set(reportName);

                    return new ExportResult(
                            reportName,
                            "test://report.pdf"
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        // Act
        ExportResult result =
                reportJob.triggerPdfExport(
                        "MONTHLY_FINANCE_REPORT"
                ).join();

        // Assert
        assertAll(
                () -> assertEquals(
                        "MONTHLY_FINANCE_REPORT",
                        receivedReportName.get()
                ),
                () -> assertEquals(
                        "MONTHLY_FINANCE_REPORT",
                        result.reportName()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that the export method returns before the background operation finishes"
    )
    void testTriggerReturnsBeforeBackgroundExportFinishes() {
        // Arrange
        CountDownLatch exportStarted =
                new CountDownLatch(1);

        CountDownLatch releaseExport =
                new CountDownLatch(1);

        ReportExportService blockingExportService =
                reportName -> {
                    exportStarted.countDown();

                    try {
                        releaseExport.await();

                        return new ExportResult(
                                reportName,
                                "test://completed.pdf"
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                };

        BackgroundReportJob reportJob =
                createReportJob(blockingExportService);

        try {
            // Act
            CompletableFuture<ExportResult> exportFuture =
                    reportJob.triggerPdfExport(
                            "BACKGROUND_REPORT"
                    );

            await()
                    .atMost(Duration.ofSeconds(1))
                    .until(() -> exportStarted.getCount() == 0);

            // Assert before releasing the background operation
            assertAll(
                    () -> assertNotNull(exportFuture),
                    () -> assertFalse(exportFuture.isDone())
            );

            releaseExport.countDown();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(exportFuture::isDone);

            assertEquals(
                    "test://completed.pdf",
                    exportFuture.join().storageLocation()
            );

        } finally {
            releaseExport.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that export start and completion messages are logged"
    )
    void testSuccessfulExportProducesExpectedLogs() {
        // Arrange
        ReportExportService fakeExportService =
                reportName -> new ExportResult(
                        reportName,
                        "s3://bucket/report.pdf"
                );

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        // Act
        reportJob.triggerPdfExport("REPORT-001").join();

        // Assert
        assertAll(
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Beginning heavy compilation matrix for: REPORT-001"
                                        )
                        )
                ),
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Report finalized and uploaded to: "
                                                        + "s3://bucket/report.pdf"
                                        )
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that background work executes on an executor worker thread"
    )
    void testExportRunsOnBackgroundWorkerThread() {
        // Arrange
        String testThreadName =
                Thread.currentThread().getName();

        AtomicReference<String> workerThreadName =
                new AtomicReference<>();

        ReportExportService fakeExportService =
                reportName -> {
                    workerThreadName.set(
                            Thread.currentThread().getName()
                    );

                    return new ExportResult(
                            reportName,
                            "test://result.pdf"
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        // Act
        reportJob.triggerPdfExport("REPORT-002").join();

        // Assert
        assertAll(
                () -> assertNotNull(workerThreadName.get()),
                () -> assertNotEquals(
                        testThreadName,
                        workerThreadName.get()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that export-service failure completes the future exceptionally"
    )
    void testExportFailureCompletesFutureExceptionally() {
        // Arrange
        ReportExportService brokenExportService =
                reportName -> {
                    throw new IllegalStateException(
                            "Cloud storage unavailable."
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(brokenExportService);

        // Act
        CompletableFuture<ExportResult> exportFuture =
                reportJob.triggerPdfExport("REPORT-003");

        await()
                .atMost(Duration.ofSeconds(2))
                .until(exportFuture::isDone);

        CompletionException exception = assertThrows(
                CompletionException.class,
                exportFuture::join
        );

        Throwable cause = rootCause(exception);

        // Assert
        assertAll(
                () -> assertTrue(
                        exportFuture.isCompletedExceptionally()
                ),
                () -> assertFalse(exportFuture.isCancelled()),
                () -> assertInstanceOf(
                        IllegalStateException.class,
                        cause
                ),
                () -> assertEquals(
                        "Cloud storage unavailable.",
                        cause.getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that completion logging is not executed after export failure"
    )
    void testFailedExportDoesNotLogSuccessfulCompletion() {
        // Arrange
        ReportExportService brokenExportService =
                reportName -> {
                    throw new IllegalStateException(
                            "PDF renderer failed."
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(brokenExportService);

        // Act
        CompletableFuture<ExportResult> exportFuture =
                reportJob.triggerPdfExport("REPORT-004");

        assertThrows(
                CompletionException.class,
                exportFuture::join
        );

        // Assert
        assertAll(
                () -> assertTrue(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Beginning heavy compilation matrix"
                                        )
                        )
                ),
                () -> assertFalse(
                        logs.stream().anyMatch(
                                message ->
                                        message.contains(
                                                "Report finalized and uploaded"
                                        )
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that whenComplete receives the successful export result"
    )
    void testWhenCompleteReceivesSuccessfulResult() {
        // Arrange
        ReportExportService fakeExportService =
                reportName -> new ExportResult(
                        reportName,
                        "test://successful-export.pdf"
                );

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        AtomicReference<ExportResult> callbackResult =
                new AtomicReference<>();

        AtomicReference<Throwable> callbackFailure =
                new AtomicReference<>();

        // Act
        CompletableFuture<ExportResult> exportFuture =
                reportJob.triggerPdfExport("REPORT-005");

        CompletableFuture<ExportResult> callbackFuture =
                exportFuture.whenComplete(
                        (result, exception) -> {
                            callbackResult.set(result);
                            callbackFailure.set(exception);
                        }
                );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(callbackFuture::isDone);

        // Assert
        assertAll(
                () -> assertNull(callbackFailure.get()),
                () -> assertNotNull(callbackResult.get()),
                () -> assertEquals(
                        "test://successful-export.pdf",
                        callbackResult.get().storageLocation()
                ),
                () -> assertSame(
                        exportFuture.join(),
                        callbackFuture.join()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that whenComplete receives the asynchronous failure"
    )
    void testWhenCompleteReceivesExportFailure() {
        // Arrange
        ReportExportService brokenExportService =
                reportName -> {
                    throw new IllegalArgumentException(
                            "Unsupported report format."
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(brokenExportService);

        AtomicReference<ExportResult> callbackResult =
                new AtomicReference<>();

        AtomicReference<Throwable> callbackFailure =
                new AtomicReference<>();

        // Act
        CompletableFuture<ExportResult> callbackFuture =
                reportJob.triggerPdfExport("REPORT-006")
                        .whenComplete(
                                (result, exception) -> {
                                    callbackResult.set(result);
                                    callbackFailure.set(exception);
                                }
                        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(callbackFuture::isDone);

        // Assert
        assertThrows(
                CompletionException.class,
                callbackFuture::join
        );

        assertAll(
                () -> assertNull(callbackResult.get()),
                () -> assertNotNull(callbackFailure.get()),
                () -> assertInstanceOf(
                        IllegalArgumentException.class,
                        rootCause(callbackFailure.get())
                ),
                () -> assertEquals(
                        "Unsupported report format.",
                        rootCause(
                                callbackFailure.get()
                        ).getMessage()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that whenComplete observes but does not recover from failure"
    )
    void testWhenCompleteDoesNotRecoverFromFailure() {
        // Arrange
        ReportExportService brokenExportService =
                reportName -> {
                    throw new IllegalStateException(
                            "Export engine stopped."
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(brokenExportService);

        AtomicBoolean callbackExecuted =
                new AtomicBoolean(false);

        // Act
        CompletableFuture<ExportResult> callbackFuture =
                reportJob.triggerPdfExport("REPORT-007")
                        .whenComplete(
                                (result, exception) ->
                                        callbackExecuted.set(true)
                        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(callbackFuture::isDone);

        // Assert
        assertAll(
                () -> assertTrue(callbackExecuted.get()),
                () -> assertTrue(
                        callbackFuture.isCompletedExceptionally()
                ),
                () -> assertThrows(
                        CompletionException.class,
                        callbackFuture::join
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that exceptionally can recover from an export failure"
    )
    void testExceptionallyRecoversFromExportFailure() {
        // Arrange
        ReportExportService brokenExportService =
                reportName -> {
                    throw new IllegalStateException(
                            "Primary export service unavailable."
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(brokenExportService);

        // Act
        CompletableFuture<ExportResult> recoveredFuture =
                reportJob.triggerPdfExport("REPORT-008")
                        .exceptionally(
                                error -> new ExportResult(
                                        "REPORT-008",
                                        "backup://reports/REPORT-008.pdf"
                                )
                        );

        await()
                .atMost(Duration.ofSeconds(2))
                .until(recoveredFuture::isDone);

        // Assert
        assertAll(
                () -> assertFalse(
                        recoveredFuture
                                .isCompletedExceptionally()
                ),
                () -> assertEquals(
                        "backup://reports/REPORT-008.pdf",
                        recoveredFuture.join()
                                .storageLocation()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify that a blank report name is rejected before task submission"
    )
    void testBlankReportNameIsRejectedImmediately() {
        // Arrange
        AtomicBoolean serviceCalled =
                new AtomicBoolean(false);

        ReportExportService fakeExportService =
                reportName -> {
                    serviceCalled.set(true);

                    return new ExportResult(
                            reportName,
                            "test://result.pdf"
                    );
                };

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> reportJob.triggerPdfExport("   ")
        );

        // Assert
        assertAll(
                () -> assertEquals(
                        "Report name is required.",
                        exception.getMessage()
                ),
                () -> assertFalse(serviceCalled.get())
        );
    }

    @Test
    @DisplayName(
            "Verify that a null report name is rejected before task submission"
    )
    void testNullReportNameIsRejectedImmediately() {
        // Arrange
        BackgroundReportJob reportJob =
                createReportJob(
                        reportName -> new ExportResult(
                                reportName,
                                "test://result.pdf"
                        )
                );

        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> reportJob.triggerPdfExport(null)
        );

        // Assert
        assertEquals(
                "Report name is required.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify that task submission after executor shutdown is rejected"
    )
    void testSubmissionAfterExecutorShutdownIsRejected() {
        // Arrange
        BackgroundReportJob reportJob =
                createReportJob(
                        reportName -> new ExportResult(
                                reportName,
                                "test://result.pdf"
                        )
                );

        executor.shutdown();

        // Act & Assert
        assertThrows(
                RejectedExecutionException.class,
                () -> reportJob.triggerPdfExport("REPORT-009")
        );
    }

    @Test
    @DisplayName(
            "Verify that multiple exports can complete concurrently"
    )
    void testMultipleExportsCompleteConcurrently() {
        // Arrange
        CountDownLatch bothTasksStarted =
                new CountDownLatch(2);

        CountDownLatch releaseTasks =
                new CountDownLatch(1);

        AtomicInteger activeTasks =
                new AtomicInteger();

        AtomicInteger maximumConcurrentTasks =
                new AtomicInteger();

        ReportExportService concurrentExportService =
                reportName -> {
                    int active = activeTasks.incrementAndGet();

                    maximumConcurrentTasks.accumulateAndGet(
                            active,
                            Math::max
                    );

                    bothTasksStarted.countDown();

                    try {
                        releaseTasks.await();

                        return new ExportResult(
                                reportName,
                                "test://" + reportName + ".pdf"
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    } finally {
                        activeTasks.decrementAndGet();
                    }
                };

        BackgroundReportJob reportJob =
                createReportJob(concurrentExportService);

        try {
            // Act
            CompletableFuture<ExportResult> firstFuture =
                    reportJob.triggerPdfExport("REPORT-A");

            CompletableFuture<ExportResult> secondFuture =
                    reportJob.triggerPdfExport("REPORT-B");

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(() -> bothTasksStarted.getCount() == 0);

            // Assert while both tasks are held
            assertEquals(
                    2,
                    maximumConcurrentTasks.get(),
                    "Both tasks should have run concurrently."
            );

            releaseTasks.countDown();

            CompletableFuture<Void> allTasks =
                    CompletableFuture.allOf(
                            firstFuture,
                            secondFuture
                    );

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(allTasks::isDone);

            assertAll(
                    () -> assertEquals(
                            "REPORT-A",
                            firstFuture.join().reportName()
                    ),
                    () -> assertEquals(
                            "REPORT-B",
                            secondFuture.join().reportName()
                    )
            );

        } finally {
            releaseTasks.countDown();
        }
    }

    @Test
    @DisplayName(
            "Verify that graceful executor shutdown allows an existing export to finish"
    )
    void testGracefulShutdownAllowsPendingExportToFinish()
            throws Exception {

        // Arrange
        ReportExportService fakeExportService =
                reportName -> new ExportResult(
                        reportName,
                        "test://graceful-shutdown.pdf"
                );

        BackgroundReportJob reportJob =
                createReportJob(fakeExportService);

        CompletableFuture<ExportResult> exportFuture =
                reportJob.triggerPdfExport("REPORT-010");

        // Act
        executor.shutdown();

        boolean terminated =
                executor.awaitTermination(
                        2,
                        TimeUnit.SECONDS
                );

        // Assert
        assertAll(
                () -> assertTrue(terminated),
                () -> assertTrue(executor.isShutdown()),
                () -> assertTrue(executor.isTerminated()),
                () -> assertTrue(exportFuture.isDone()),
                () -> assertFalse(exportFuture.isCancelled()),
                () -> assertFalse(
                        exportFuture.isCompletedExceptionally()
                ),
                () -> assertEquals(
                        "test://graceful-shutdown.pdf",
                        exportFuture.join().storageLocation()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null executor"
    )
    void testBackgroundReportJobRejectsNullExecutor() {
        // Act & Assert
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new BackgroundReportJob(
                        null,
                        reportName -> new ExportResult(
                                reportName,
                                "test://result.pdf"
                        ),
                        logs::add
                )
        );

        assertEquals(
                "executor must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null export service"
    )
    void testBackgroundReportJobRejectsNullExportService() {
        // Act & Assert
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new BackgroundReportJob(
                        executor,
                        null,
                        logs::add
                )
        );

        assertEquals(
                "exportService must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify constructor rejects a null logger"
    )
    void testBackgroundReportJobRejectsNullLogger() {
        // Act & Assert
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new BackgroundReportJob(
                        executor,
                        reportName -> new ExportResult(
                                reportName,
                                "test://result.pdf"
                        ),
                        null
                )
        );

        assertEquals(
                "logger must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify ExportResult rejects a null report name"
    )
    void testExportResultRejectsNullReportName() {
        // Act & Assert
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new ExportResult(
                        null,
                        "test://result.pdf"
                )
        );

        assertEquals(
                "reportName must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify ExportResult rejects a null storage location"
    )
    void testExportResultRejectsNullStorageLocation() {
        // Act & Assert
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new ExportResult(
                        "REPORT",
                        null
                )
        );

        assertEquals(
                "storageLocation must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify simulated export service creates the expected storage location"
    )
    void testSimulatedExportServiceCreatesExpectedResult() {
        // Arrange
        SimulatedPdfExportService exportService =
                new SimulatedPdfExportService(
                        Duration.ZERO
                );

        // Act
        ExportResult result =
                exportService.export(
                        "ANNUAL_REPORT"
                );

        // Assert
        assertAll(
                () -> assertEquals(
                        "ANNUAL_REPORT",
                        result.reportName()
                ),
                () -> assertEquals(
                        "s3://secure-reports/ANNUAL_REPORT.pdf",
                        result.storageLocation()
                )
        );
    }

    @Test
    @DisplayName(
            "Verify simulated export service rejects negative processing delay"
    )
    void testSimulatedExportServiceRejectsNegativeDelay() {
        // Act
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new SimulatedPdfExportService(
                        Duration.ofMillis(-1)
                )
        );

        // Assert
        assertEquals(
                "Processing delay cannot be negative.",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify simulated export service rejects null processing delay"
    )
    void testSimulatedExportServiceRejectsNullDelay() {
        // Act
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new SimulatedPdfExportService(null)
        );

        // Assert
        assertEquals(
                "processingDelay must not be null",
                exception.getMessage()
        );
    }

    @Test
    @DisplayName(
            "Verify interruption of simulated export is reported through CompletionException"
    )
    void testSimulatedExportReportsInterruption() {
        // Arrange
        SimulatedPdfExportService exportService =
                new SimulatedPdfExportService(
                        Duration.ofSeconds(30)
                );

        ExecutorService singleExecutor =
                Executors.newSingleThreadExecutor();

        try {
            CompletableFuture<ExportResult> exportFuture =
                    CompletableFuture.supplyAsync(
                            () -> exportService.export(
                                    "LONG_REPORT"
                            ),
                            singleExecutor
                    );

            await()
                    .atMost(Duration.ofSeconds(1))
                    .until(() -> !exportFuture.isDone());

            // Act: shutdownNow interrupts the worker sleeping
            singleExecutor.shutdownNow();

            await()
                    .atMost(Duration.ofSeconds(2))
                    .until(exportFuture::isDone);

            CompletionException exception = assertThrows(
                    CompletionException.class,
                    exportFuture::join
            );

            Throwable cause = rootCause(exception);

            // Assert
            assertAll(
                    () -> assertInstanceOf(
                            InterruptedException.class,
                            cause
                    ),
                    () -> assertEquals(
                            "sleep interrupted",
                            cause.getMessage()
                    )
            );

        } finally {
            singleExecutor.shutdownNow();
        }
    }

    private BackgroundReportJob createReportJob(
            ReportExportService exportService
    ) {
        return new BackgroundReportJob(
                executor,
                exportService,
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
