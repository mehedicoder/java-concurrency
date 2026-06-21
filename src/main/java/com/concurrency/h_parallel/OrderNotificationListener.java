package com.concurrency.h_parallel;

/**
 * Functional interface contract utilized by the OrderProcessingEngine
 * to dispatch non-blocking callbacks to registered subscribers.
 */
@FunctionalInterface
public interface OrderNotificationListener {

    /**
     * Triggered automatically whenever a backend worker thread 
     * successfully processes an order from the queue.
     *
     * @param order The processed order payload
     */
    void onOrderProcessed(OrderProcessingEngine.Order order);
}