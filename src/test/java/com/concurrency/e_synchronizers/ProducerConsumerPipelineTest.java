package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ProducerConsumerPipelineTest {

    @Test
    @DisplayName("Scenario 1: Cluster Balancing - Verified workload processing under rule (# Producers < # Consumers)")
    void testClusterLoadBalancingAndGracefulDrain() throws InterruptedException {
        // Arrange
        int maxBufferCapacity = 10;
        TelemetryLogBuffer sharedBuffer = new TelemetryLogBuffer(maxBufferCapacity);

        int producerCount = 2;
        int consumerCount = 5; // Satisfies rule: 2 < 5
        int itemsPerProducer = 10;
        int totalExpectedItems = producerCount * itemsPerProducer;

        AtomicInteger processedItemCount = new AtomicInteger(0);

        // Define a fast producer task
        Runnable producerTask = () -> {
            try {
                for (int i = 0; i < itemsPerProducer; i++) {
                    sharedBuffer.enqueueLog(new LogMessage("Test Packet Data"));
                    Thread.sleep(10); // Rapid burst velocity
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // Define consumer task that tracks its global work count
        Runnable consumerTask = () -> {
            try {
                while (true) {
                    LogMessage log = sharedBuffer.dequeueLog();
                    if (log != null && log.isShutdownSignal()) {
                        break;
                    }
                    processedItemCount.incrementAndGet();
                    Thread.sleep(50); // Intentionally slower than producers
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        List<Thread> producers = new ArrayList<>();
        List<Thread> consumers = new ArrayList<>();

        // Act: Start threads concurrently
        for (int i = 0; i < consumerCount; i++) {
            Thread c = new Thread(consumerTask);
            consumers.add(c);
            c.start();
        }

        for (int i = 0; i < producerCount; i++) {
            Thread p = new Thread(producerTask);
            producers.add(p);
            p.start();
        }

        // Await producer finalization
        for (Thread p : producers) {
            p.join(2000);
        }

        // Trigger multi-token broadcast shutdown sequence
        sharedBuffer.broadcastShutdown(consumerCount);

        // Await consumer lifecycle wrap ups
        for (Thread c : consumers) {
            c.join(2000);
            assertFalse(c.isAlive(), "Consumer thread deadlocked or failed to process its shutdown signal.");
        }

        // Assert
        assertEquals(totalExpectedItems, processedItemCount.get(),
                "The consumer cluster did not process the exact total volume of items produced by the burst nodes.");
        assertEquals(0, sharedBuffer.getCurrentSize(), "The log buffer queue was not completely drained.");
    }

    @Test
    @DisplayName("Scenario 2: Multi-Token Shutdown Guard - Every consumer thread receives its shutdown signal")
    void testMultiTokenBroadcastWakesAllConsumers() throws InterruptedException {
        // Arrange
        TelemetryLogBuffer sharedBuffer = new TelemetryLogBuffer(5);
        int totalConsumers = 4;
        List<Thread> consumers = new ArrayList<>();

        // Consumers wait on an empty queue immediately
        Runnable blockingConsumerTask = () -> {
            try {
                while (true) {
                    LogMessage log = sharedBuffer.dequeueLog();
                    if (log != null && log.isShutdownSignal()) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        for (int i = 0; i < totalConsumers; i++) {
            Thread c = new Thread(blockingConsumerTask);
            consumers.add(c);
            c.start();
        }

        // Let threads enter WAITING state inside buffer.take()
        Thread.sleep(200);

        for (Thread c : consumers) {
            assertEquals(Thread.State.WAITING, c.getState(),
                    "Consumer failed to transition to a WAITING state while idling on an empty queue.");
        }

        // Act: Broadcast exact number of tokens
        sharedBuffer.broadcastShutdown(totalConsumers);

        // Assert: Verify all threads unlock and terminate cleanly without hanging the test
        for (Thread c : consumers) {
            c.join(1000);
            assertFalse(c.isAlive(), "A consumer thread was left stranded or missed its shutdown token.");
        }
    }
}
