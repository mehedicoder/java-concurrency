package com.concurrency.a_thread_process;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * ##ä
 * Business Logic encapsulated into a reusable, testable Runnable.
 */
class LogAnalysisTask implements Runnable {
    private final String logFileName;
    private final List<String> mockLogLines; // Used for predictable testing
    private final int maxIterations;         // Added to track execution bounds
    private long errorsFound = 0;

    // Constructor for real-world file simulation (Defaults to 10M iterations)
    public LogAnalysisTask(String logFileName) {
        this(logFileName, null, 10_000_000);
    }

    // Constructor for Unit Testing with a list (Defaults to 0 iterations since list handles it)
    public LogAnalysisTask(String logFileName, List<String> mockLogLines) {
        this(logFileName, mockLogLines, 0);
    }

    // Master Constructor that enforces boundary conditions
    public LogAnalysisTask(String logFileName, List<String> mockLogLines, int maxIterations) {
        if (logFileName == null) {
            throw new IllegalArgumentException("Log file name cannot be null");
        }
        // This is the check your test is looking for!
        if (maxIterations < 0) {
            throw new IllegalArgumentException("Iteration count cannot be negative");
        }
        this.logFileName = logFileName;
        this.mockLogLines = mockLogLines;
        this.maxIterations = maxIterations;
    }

    public long getErrorsFound() {
        return errorsFound;
    }

    @Override
    public void run() {
        // Defensive Check: Null validation
        if (logFileName == null) {
            throw new IllegalArgumentException("Log file name cannot be null");
        }

        System.out.format("[START] Processing: %s\n", logFileName);

        // Scenario A: Test Mode (Using injected lines)
        if (mockLogLines != null) {
            for (String line : mockLogLines) {
                if (Thread.currentThread().isInterrupted()) return;
                processLine(line);
            }
        }
        // Scenario B: Real/Simulation Mode (Heavy CPU loop dynamically sized)
        else {
            for (int i = 0; i < maxIterations; i++) {
                if (Thread.currentThread().isInterrupted()) return;

                // Simulate generating/reading a line
                if (i % 1_000_000 == 0) {
                    processLine("2026-06-13 [ERROR] Timeout at step " + i);
                }
            }
        }

        System.out.format("[SUCCESS] Finished %s. Found %d errors.\n", logFileName, errorsFound);
    }

    // Isolated core logic for easy testing
    public void processLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return; // Gracefully handle null/empty lines instead of crashing
        }
        if (line.contains("[ERROR]")) {
            errorsFound++;
        }
    }
}

public class A_ThreadProcess {
    public static void main(String[] args) {
        System.out.println("Starting Log Analysis via AutoCloseable ExecutorService...\n");

        List<LogAnalysisTask> tasks = new ArrayList<>();


        // Try-with-resources: ExecutorService handles its own closing/shutdown sequence automatically
        try (ExecutorService executor = Executors.newFixedThreadPool(3)) {

            for (int i = 1; i <= 6; i++) {
                LogAnalysisTask task = new LogAnalysisTask("server_log_" + i + ".log");
                tasks.add(task);

                // Submit to executor; handles potential RejectedExecutionException
                executor.submit(task);
            }

            System.out.println("All tasks submitted. Waiting for executor auto-close...\n");
            // The block won't exit until all threads finish OR an exception forces it out.

        } catch (RejectedExecutionException e) {
            System.err.println("Task submission rejected: " + e.getMessage());
        } catch (SecurityException e) {
            System.err.println("Security policy restricted thread pool allocation: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred during execution: " + e.getMessage());
        }

        System.out.println("\nExecution complete. Aggregating data:");
        for (LogAnalysisTask task : tasks) {
            System.out.format("Task completed with %d errors counted.\n", task.getErrorsFound());
        }
    }
}