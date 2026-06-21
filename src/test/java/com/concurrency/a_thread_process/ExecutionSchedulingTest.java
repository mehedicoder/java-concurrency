package com.concurrency.a_thread_process;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.Queue;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionSchedulingTest {

    @Test
    @DisplayName("Happy Path: Multiple processors should drain the queue completely without race conditions")
    void testHappyPathMultipleProcessors() throws InterruptedException {
        // Arrange
        Queue<Order> queue = new ConcurrentLinkedQueue<>();
        int totalOrders = 10;
        for (int i = 1; i <= totalOrders; i++) {
            queue.add(new Order("ORD-" + i));
        }

        OrderProcessor processorA = new OrderProcessor("Alpha", queue);
        OrderProcessor processorB = new OrderProcessor("Beta", queue);

        // Act
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            executor.execute(processorA);
            executor.execute(processorB);
            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);

            // Assert
            assertTrue(finished, "Executor should have finished processing within 5 seconds");
            assertTrue(queue.isEmpty(), "The order queue should be completely drained");

            int combinedCount = processorA.getProcessedCount() + processorB.getProcessedCount();
            assertEquals(totalOrders, combinedCount, "Combined processed count must equal total orders submitted");
        }
    }

    @Test
    @DisplayName("Edge Case: Processing should gracefully handle an immediately empty queue")
    void testEmptyQueue() throws InterruptedException {
        // Arrange
        Queue<Order> emptyQueue = new ConcurrentLinkedQueue<>();
        OrderProcessor processor = new OrderProcessor("Alpha", emptyQueue);

        // Act
        Thread thread = new Thread(processor);
        thread.start();
        thread.join(2000); // Wait up to 2 seconds for it to exit naturally

        // Assert
        assertFalse(thread.isAlive(), "Thread should have terminated immediately on empty queue");
        assertEquals(0, processor.getProcessedCount(), "Processed count should be exactly 0");
    }

    @Test
    @DisplayName("Edge Case: Processing should handle missing/null values inside the queue gracefully")
    void testQueueContainingNullValues() {
        // Arrange
        Queue<Order> queue = new ConcurrentLinkedQueue<>();

        // Note: ConcurrentLinkedQueue doesn't allow null elements (throws NPE on add).
        // However, if a standard thread-safe wrapper or custom queue that allows nulls is passed:
        queue.add(new Order(null)); // Order ID is null (missing value)

        OrderProcessor processor = new OrderProcessor("Alpha", queue);

        // Act & Assert
        assertDoesNotThrow(() -> {
            Thread thread = new Thread(processor);
            thread.start();
            thread.join(2000);
        }, "Should not throw a NullPointerException when processing an Order with a null orderId");

        assertEquals(1, processor.getProcessedCount(), "The order with null properties should still be processed");
    }

    @Test
    @DisplayName("Exception/Edge Case: Null validation on constructor arguments")
    void testNullConstructorArguments() {
        // Arrange & Act & Assert
        // If your business rule demands these shouldn't be null, they should fail-fast.
        // Let's test how the current implementation reacts if the queue is null.
        OrderProcessor processorWithNullQueue = new OrderProcessor("Alpha", null);

        assertThrows(NullPointerException.class, processorWithNullQueue::run, "Invoking run() with a null queue reference should result in a NullPointerException");
    }

    @Test
    @DisplayName("Exception Test: Thread should gracefully exit loop if interrupted mid-sleep")
    void testThreadInterruptionHandling() throws InterruptedException {
        // Arrange
        Queue<Order> queue = new ConcurrentLinkedQueue<>();
        queue.add(new Order("DELAY-ORD-1"));
        queue.add(new Order("DELAY-ORD-2")); // This one shouldn't get processed if interrupted

        OrderProcessor processor = new OrderProcessor("Alpha", queue);
        Thread workerThread = new Thread(processor);

        // Act
        workerThread.start();

        // Give it a tiny fraction of a second to start running up to the Thread.sleep(50) block
        Thread.sleep(10);

        // Interrupt it while it is sleeping inside the run() loop
        workerThread.interrupt();
        workerThread.join(2000);

        // Assert
        assertFalse(workerThread.isAlive(), "Worker thread should have shut down following the interruption signal");
        assertEquals(1, processor.getProcessedCount(), "Processor should have caught the interrupt signal and stopped after 1 item");
    }
}