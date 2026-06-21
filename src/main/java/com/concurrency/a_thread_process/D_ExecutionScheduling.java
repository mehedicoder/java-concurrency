package com.concurrency.a_thread_process;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a customer order task to be processed asynchronously.
 */
record Order(String orderId) {
}

/**
 * The worker task assigned to the Thread Pool.
 * Continuously processes orders from the shared queue until empty.
 */
class OrderProcessor implements Runnable {
    private final Queue<Order> orderQueue;
    private final String processorName;
    private final AtomicInteger processedCount = new AtomicInteger(0);

    public OrderProcessor(String processorName, Queue<Order> orderQueue) {
        this.processorName = processorName;
        this.orderQueue = orderQueue;
    }

    @Override
    public void run() {
        Order order;
        // Safely poll the queue. If it's empty, poll() returns null.
        while ((order = orderQueue.poll()) != null) {
            System.out.format("[Thread: %s] %s successfully processed order: %s\n",
                    Thread.currentThread().getName(), processorName, order.orderId());

            // Atomically increment without lock overhead
            processedCount.incrementAndGet();

            // Simulate brief network/DB latency for processing an order
            try {
                Thread.sleep(50);
            } catch (IllegalArgumentException ie) {
                System.err.println("[Thread: " + Thread.currentThread().getName() + "] Invalid sleep duration: " + ie.getMessage());
            } catch (InterruptedException ee) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                break;
            }
        }
    }

    public int getProcessedCount() {
        return processedCount.get();
    }
}

public class D_ExecutionScheduling {
    public static void main(String[] args) throws InterruptedException {
        // 1. Create a thread-safe FIFO queue and populate it with simulated orders
        Queue<Order> globalOrderQueue = new ConcurrentLinkedQueue<>();
        for (int i = 1; i <= 20; i++) {
            globalOrderQueue.add(new Order("ORD-ID-" + i));
        }

        // 2. Initialize our worker objects with access to the shared queue
        OrderProcessor processorA = new OrderProcessor("Processor-Alpha", globalOrderQueue);
        OrderProcessor processorB = new OrderProcessor("Processor-Beta", globalOrderQueue);

        // 3. Use an ExecutorService to manage our worker threads
        // We have 2 workers, so we provision a pool of 2 threads.
        ExecutorService executor = Executors.newFixedThreadPool(2);

        System.out.println("--- Launching Parallel Order Processing System ---");

        // Submit tasks to the thread pool
        executor.execute(processorA);
        executor.execute(processorB);

        // 4. Properly lifecycle manage the thread pool
        executor.shutdown(); // Disable new tasks from being submitted

        // Wait up to 1 minute for existing tasks to finish executing (similar to join())
        if (executor.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.println("\n--- All orders processed successfully! ---");
            System.out.format("Processor Alpha handled %d orders.\n", processorA.getProcessedCount());
            System.out.format("Processor Beta handled %d orders.\n", processorB.getProcessedCount());
        } else {
            System.out.println("System timed out before processing completed.");
        }
    }
}