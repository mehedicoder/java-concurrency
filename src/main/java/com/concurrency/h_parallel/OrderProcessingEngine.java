package com.concurrency.h_parallel;

import com.concurrency.h_parallel.OrderNotificationListener;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class OrderProcessingEngine {

    public record Order(String orderId, String sku, int quantity) {}
    public record AuditSnapshot(String message) {}

    // ─── THE COLLISION CURE ───
    // Composite key ensures uniqueness even if generated at the exact same nanosecond
    public record AuditKey(Instant timestamp, long sequence) implements Comparable<AuditKey> {
        @Override
        public int compareTo(AuditKey o) {
            int timeCompare = this.timestamp.compareTo(o.timestamp);
            if (timeCompare != 0) return timeCompare;
            return Long.compare(this.sequence, o.sequence); // Tie-breaker!
        }
    }

    private final AtomicLong sequenceGenerator = new AtomicLong(0);
    private final BlockingQueue<Order> orderBufferQueue = new LinkedBlockingQueue<>(10_000);
    private final ConcurrentHashMap<String, LongAdder> skuMetricsMap = new ConcurrentHashMap<>();

    // Updated map signature to use our composite unique key
    private final ConcurrentSkipListMap<AuditKey, AuditSnapshot> chronologicalAuditLog = new ConcurrentSkipListMap<>();
    private final List<OrderNotificationListener> eventListeners = new CopyOnWriteArrayList<>();

    public void registerListener(OrderNotificationListener listener) {
        eventListeners.add(listener);
    }

    public void submitIncomingOrder(Order order) {
        orderBufferQueue.offer(order);
    }

    public void processNextOrder() throws InterruptedException {
        Order order = orderBufferQueue.poll(500, TimeUnit.MILLISECONDS);
        if (order == null) return;

        skuMetricsMap.computeIfAbsent(order.sku(), k -> new LongAdder()).add(order.quantity());

        // Generate guaranteed unique key combination
        AuditKey uniqueKey = new AuditKey(Instant.now(), sequenceGenerator.incrementAndGet());
        AuditSnapshot logEntry = new AuditSnapshot("Processed order #" + order.orderId());
        chronologicalAuditLog.put(uniqueKey, logEntry);

        for (OrderNotificationListener listener : eventListeners) {
            listener.onOrderProcessed(order);
        }
    }

    public long getAggregatedSkuCount(String sku) {
        LongAdder adder = skuMetricsMap.get(sku);
        return (adder != null) ? adder.sum() : 0L;
    }

    public ConcurrentSkipListMap<AuditKey, AuditSnapshot> getAuditLog() {
        return this.chronologicalAuditLog;
    }

    public int getQueueSize() {
        return orderBufferQueue.size();
    }
}