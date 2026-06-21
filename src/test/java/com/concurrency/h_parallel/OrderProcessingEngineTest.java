package com.concurrency.h_parallel;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OrderProcessingEngineTest {

    @Test
    @DisplayName("Verify thread-safety and ordering integrity under parallel multi-threaded load")
    void testParallelPipelineExecution() throws InterruptedException {
        // Arrange
        OrderProcessingEngine engine = new OrderProcessingEngine();
        AtomicInteger notificationCounter = new AtomicInteger(0);

        // Register a listener into our CopyOnWriteArrayList
        engine.registerListener(order -> notificationCounter.incrementAndGet());

        int taskCount = 1000;
        ExecutorService clientPool = Executors.newFixedThreadPool(16);
        CountDownLatch coordinationLatch = new CountDownLatch(taskCount);

        // Act: Phase 1 - Simultaneously populate the LinkedBlockingQueue from parallel threads
        for (int i = 0; i < taskCount; i++) {
            final int id = i;
            clientPool.submit(() -> {
                engine.submitIncomingOrder(new OrderProcessingEngine.Order("ID-" + id, "IPHONE-17", 2));
                coordinationLatch.countDown();
            });
        }
        coordinationLatch.await(); // Sync point ensuring all data is queued
        assertEquals(taskCount, engine.getQueueSize(), "LinkedBlockingQueue dropped elements during parallel load.");

        // Act: Phase 2 - Spin up parallel consumers to drain the queue simultaneously
        CountDownLatch consumerLatch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            clientPool.submit(() -> {
                try {
                    engine.processNextOrder();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    consumerLatch.countDown();
                }
            });
        }
        consumerLatch.await(); // Wait for processing to conclude
        clientPool.shutdown();

        // Assertions
        // 1. Verify ConcurrentHashMap accumulated calculations perfectly
        long totalItemsSold = engine.getAggregatedSkuCount("IPHONE-17");
        assertEquals(taskCount * 2L, totalItemsSold, "ConcurrentHashMap atomic aggregation totals are wrong.");

        // 2. Verify CopyOnWriteArrayList safely triggered notifications without race conditions
        assertEquals(taskCount, notificationCounter.get(), "CopyOnWriteArrayList loop drops encountered.");

        // 3. Verify ConcurrentSkipListMap preserved chronological ordering keys seamlessly
        var auditLog = engine.getAuditLog();
        assertEquals(taskCount, auditLog.size(), "ConcurrentSkipListMap sizes don't match processed totals.");

        // Java 21+ SequencedMap confirmation: Chronological order implies firstKey is strictly before lastKey
        Instant oldestTimestamp = auditLog.firstKey().timestamp();
        Instant newestTimestamp = auditLog.lastKey().timestamp();
        assertTrue(oldestTimestamp.isBefore(newestTimestamp) || oldestTimestamp.equals(newestTimestamp),
                "ConcurrentSkipListMap failed sorting properties.");
    }
}