package com.concurrency.i_measurement;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import static org.junit.jupiter.api.Assertions.*;

class ParallelImageProcessorTest {

    @Test
    void testParallelAndSequentialYieldIdenticalChecksum() {
        List<String> testBatch = List.of("img-data-1", "img-data-2", "img-data-3", "img-data-4");

        long sequentialSum = ParallelImageProcessor.processSequentially(testBatch, 0, testBatch.size());

        ForkJoinPool testPool = new ForkJoinPool(2);
        long parallelSum = testPool.invoke(new ParallelImageProcessor.ImageBatchTask(testBatch, 0, testBatch.size()));
        testPool.shutdown();

        assertEquals(sequentialSum, parallelSum, "Parallel algorithm broken! Matrix calculations mismatched.");
    }
}