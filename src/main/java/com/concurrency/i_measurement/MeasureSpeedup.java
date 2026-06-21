package com.concurrency.i_measurement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

class ParallelImageProcessor {

    // Threshold: Process directly if the batch has fewer than 15 images
    private static final int SEQUENTIAL_THRESHOLD = 15;

    /**
     * Parallel Fork-Join Task implementation
     */
    public static class ImageBatchTask extends RecursiveTask<Long> {
        private final List<String> mockImageByteStreams;
        private final int startIdx;
        private final int endIdx;

        public ImageBatchTask(List<String> mockImageByteStreams, int startIdx, int endIdx) {
            this.mockImageByteStreams = mockImageByteStreams;
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        protected Long compute() {
            int workSize = endIdx - startIdx;
            if (workSize <= SEQUENTIAL_THRESHOLD) {
                // Base case: execute sequentially on this slice
                return processSequentially(mockImageByteStreams, startIdx, endIdx);
            } else {
                // Recursive case: divide and conquer
                int midIdx = startIdx + (workSize / 2);

                ImageBatchTask leftTask = new ImageBatchTask(mockImageByteStreams, startIdx, midIdx);
                ImageBatchTask rightTask = new ImageBatchTask(mockImageByteStreams, midIdx, endIdx);

                leftTask.fork(); // Dispatch left half to another ForkJoin pool worker

                long rightResult = rightTask.compute(); // Current thread runs right half
                long leftResult = leftTask.join();     // Sync barrier to wait for left half

                return leftResult + rightResult;
            }
        }
    }

    /**
     * Sequential execution baseline
     */
    public static long processSequentially(List<String> images, int start, int end) {
        long totalProcessingCheckSum = 0;
        try {
            // SHA-512 is heavy and scales based on string size
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            for (int i = start; i < end; i++) {
                byte[] hash = digest.digest(images.get(i).getBytes(StandardCharsets.UTF_8));
                // Simulate intensive mutation matrix mathematics on the cryptographic output
                for (byte b : hash) {
                    totalProcessingCheckSum += Math.abs(b);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Cryptographic failure", e);
        }
        return totalProcessingCheckSum;
    }
}

public class MeasureSpeedup {

    public static void main(String[] args) {
        final int NUM_EVAL_RUNS = 5;
        final int TOTAL_IMAGES = 400; // Large enough to stress multiple CPU cores

        System.out.println("Generating heavy e-commerce raw image data array simulation...");
        List<String> imageDataset = new ArrayList<>(TOTAL_IMAGES);
        for (int i = 0; i < TOTAL_IMAGES; i++) {
            // Generate distinct mock text-image files to prevent CPU cache bypass tricks
            imageDataset.add("RAW_IMAGE_BYTE_DATA_STREAM_MATRIX_INDEX_" + i + "_BLOCK_" + "A".repeat(15_000));
        }

        System.out.println("Warming up JVM compilers...");
        long seqResult = ParallelImageProcessor.processSequentially(imageDataset, 0, imageDataset.size());
        ForkJoinPool pool = ForkJoinPool.commonPool();
        long parResult = pool.invoke(new ParallelImageProcessor.ImageBatchTask(imageDataset, 0, imageDataset.size()));

        if (seqResult != parResult) {
            throw new IllegalStateException("Verification failed: Results mismatch!");
        }

        // 1. Measure Sequential Timing Baseline
        System.out.println("\nEvaluating Sequential Engine...");
        double totalSequentialTime = 0;
        for (int i = 0; i < NUM_EVAL_RUNS; i++) {
            long start = System.nanoTime();
            ParallelImageProcessor.processSequentially(imageDataset, 0, imageDataset.size());
            totalSequentialTime += (System.nanoTime() - start) / 1_000_000.0; // Convert to ms
        }
        double avgSequentialTime = totalSequentialTime / NUM_EVAL_RUNS;

        // 2. Measure Parallel Execution Speed via ForkJoinPool Common Pool
        System.out.println("Evaluating Parallel ForkJoin Pool...");
        double totalParallelTime = 0;
        for (int i = 0; i < NUM_EVAL_RUNS; i++) {
            long start = System.nanoTime();
            pool.invoke(new ParallelImageProcessor.ImageBatchTask(imageDataset, 0, imageDataset.size()));
            totalParallelTime += (System.nanoTime() - start) / 1_000_000.0;
        }
        double avgParallelTime = totalParallelTime / NUM_EVAL_RUNS;

        // 3. Speedup Metrics Calculation
        int processors = Runtime.getRuntime().availableProcessors();
        double speedup = avgSequentialTime / avgParallelTime;
        double efficiency = (speedup / processors) * 100;

        System.out.println("\n--- Metric Performance Insights ---");
        System.out.format("Available CPU Compute Cores : %d\n", processors);
        System.out.format("Average Sequential Run Time: %.2f ms\n", avgSequentialTime);
        System.out.format("Average Parallel Run Time  : %.2f ms\n", avgParallelTime);
        System.out.format("Actual Algorithm Speedup    : %.2f x\n", speedup);
        System.out.format("Multi-Core Efficiency Factor: %.2f%%\n", efficiency);
    }
}