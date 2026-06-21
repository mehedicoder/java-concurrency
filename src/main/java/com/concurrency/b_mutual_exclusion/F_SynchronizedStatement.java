package com.concurrency.b_mutual_exclusion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class EventInventory {
    // Shared state tracking total booked seats across the entire system
    private static int totalSeatsBooked = 0;

    // Monitor object used explicitly for synchronization control
    private static final Object lock = new Object();

    public void bookSeat(String userIp) {
        // Simulating some realistic, non-critical background processing (e.g., logging)
        // This runs completely in parallel across threads for high performance.
        String logMessage = "Processing seat booking request from IP: " + userIp;

        // CRITICAL SECTION: We only synchronize the precise moment we mutate the shared state.
        // Instead of locking 'EventInventory.class', we lock on a dedicated private monitor object.
        synchronized (lock) {
            totalSeatsBooked++;
        }
    }

    public int getTotalSeatsBooked() {
        synchronized (lock) {
            return totalSeatsBooked;
        }
    }
}

public class F_SynchronizedStatement {
    public static void main(String[] args) {
        EventInventory event = new EventInventory();
        int bookingsPerRegion = 10_000_000;

        // Using try-with-resources to automatically manage and shut down our thread pool
        try (ExecutorService ticketServerPool = Executors.newFixedThreadPool(8)) {

            // Region A Server Handling 10 Million Requests
            ticketServerPool.submit(() -> {
                for (int i = 0; i < bookingsPerRegion; i++) {
                    event.bookSeat("192.168.1.50");
                }
            });

            // Region B Server Handling 10 Million Requests Simultaneously
            ticketServerPool.submit(() -> {
                for (int i = 0; i < bookingsPerRegion; i++) {
                    event.bookSeat("10.0.0.12");
                }
            });

        } // The pool automatically closes here, blocking until all 20 million seats are securely booked.

        // --- Production Output ---
        int expectedBookings = bookingsPerRegion * 2;
        System.out.println("====== TICKET BOOKING SYSTEM ======");
        System.out.println("Expected Seats Booked: " + expectedBookings);
        System.out.println("Actual Seats Booked:   " + event.getTotalSeatsBooked());

        if (event.getTotalSeatsBooked() == expectedBookings) {
            System.out.println("\nSUCCESS: The synchronized statement successfully guarded the critical section!");
        }
    }
}