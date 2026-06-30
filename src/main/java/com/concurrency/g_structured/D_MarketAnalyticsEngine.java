package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

 /**
 *
 * With ExecutorService and CompletableFuture, one would have to manually manage the lifecycle
 * of each task, handle exceptions, and ensure proper cleanup. StructuredTaskScope simplifies
 * this by providing a structured way to manage related tasks as a single unit of work.
 * Goal is to implement a custom solution to handle the lifecycle of a complex pipeline
 * that involves both I/O and CPU-bound tasks.
 */
public class D_MarketAnalyticsEngine {

    private final MarketDataClient dataClient;
    private final MathAnalysisEngine mathEngine;
    private final ThreadFactory vThreadFactory;
    private final ThreadFactory pThreadFactory;

    public D_MarketAnalyticsEngine(MarketDataClient dataClient, MathAnalysisEngine mathEngine,
                                   ThreadFactory vThreadFactory, ThreadFactory pThreadFactory) {
        this.dataClient = dataClient;
        this.mathEngine = mathEngine;
        this.vThreadFactory = vThreadFactory;
        this.pThreadFactory = pThreadFactory;
    }

    /**
     * Executes the combined lifecycle. Collects large data over I/O, then parses it via heavy CPU.
     */

    public record MarketSignal(String ticker, String dataDump, String statisticalTrend) {}

    public MarketSignal generateTradeSignal(String ticker, Duration deadline) throws Exception {

        // PARENT SCOPE: Dedicated to I/O-heavy tasks via lightweight Virtual Threads
        // Orchestration: If any task fails, the scope throws an exception.
        // Successful tasks are joined and their results are returned.
        try (var ioScope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                cfg -> cfg.withThreadFactory(vThreadFactory).withTimeout(deadline))) {

            System.out.format("[%s] Parent IO Scope started for %s\n", Thread.currentThread().getName(), ticker);

            // Forking off I/O data streams onto lightweight virtual worker pools
            Subtask<String> primarySource = ioScope.fork(() -> dataClient.fetchHistoricalLogs(ticker));

            // Wait for our network data to finish pouring in over the sockets
            ioScope.join();
            String rawJsonDump = primarySource.get();

            // NESTED CHILD SCOPE: Spawned specifically to pin CPU heavy math onto real OS Platform threads
            // Orchestration: If any task fails, the scope throws an exception.
            // Successful tasks are joined and their results are returned.
            try (var cpuScope = StructuredTaskScope.open(
                    StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                    cfg -> cfg.withThreadFactory(pThreadFactory))) {

                System.out.format("[%s] Nested CPU Scope executing intensive algorithmic parsing...\n", Thread.currentThread().getName());

                // Fork heavy computation off to genuine OS processing threads
                Subtask<String> computedAnalysis = cpuScope.fork(() -> mathEngine.runFourierTransformAnalysis(rawJsonDump));

                cpuScope.join(); // Synchronization barrier for math threads

                return new MarketSignal(ticker, rawJsonDump, computedAnalysis.get());
            }

        } catch (StructuredTaskScope.FailedException e) {
            if (e.getCause() instanceof Exception businessEx) throw businessEx;
            throw new RuntimeException("Pipeline orchestration failed", e);
        }
    }
}

// Service interfaces
interface MarketDataClient { String fetchHistoricalLogs(String ticker) throws Exception; }
interface MathAnalysisEngine { String runFourierTransformAnalysis(String data) throws Exception; }
