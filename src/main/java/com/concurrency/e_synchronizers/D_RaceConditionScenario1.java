package com.concurrency.e_synchronizers;

class FlightReservationEngine {
    private int totalBookings = 0;
    private int availableSeats = 1; // Only ONE seat left for the flight!

    // =========================================================================
    // 1. WITHOUT SYNCHRONIZATION: Low-level Data Race Condition
    // =========================================================================
    public void incrementBookingCountUnSynchronized() {
        // Lost updates will occur here because the 3 CPU steps are completely unprotected
        totalBookings++;
    }

    // =========================================================================
    // 2. WITH SYNCHRONIZATION: High-level Check-Then-Act Race Condition
    // =========================================================================
    public synchronized boolean isSeatAvailable() {
        return availableSeats > 0;
    }

    public synchronized void reserveSeat() {
        availableSeats--;
    }

    // Getters for test inspection
    public int getTotalBookings() { return totalBookings; }
    public int getAvailableSeats() { return availableSeats; }
}

public class D_RaceConditionScenario1 {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== TRACK 1: Low-Level Race Condition (No Synchronization) ===");
        FlightReservationEngine engineTrack1 = new FlightReservationEngine();

        Runnable rawCountTask = () -> {
            for (int i = 0; i < 10000; i++) {
                engineTrack1.incrementBookingCountUnSynchronized();
            }
        };

        Thread t1 = new Thread(rawCountTask, "Counter-Node-A");
        Thread t2 = new Thread(rawCountTask, "Counter-Node-B");
        t1.start(); t2.start();
        t1.join();  t2.join();

        System.out.println("Expected booking count modifications: 20000");
        System.out.format("ACTUAL booking count modifications  : %d\n", engineTrack1.getTotalBookings());
        System.out.format("Lost data packets observed: %d\n\n", (20000 - engineTrack1.getTotalBookings()));


        System.out.println("=== TRACK 2: High-Level Race Condition (With Individual Synchronization) ===");
        FlightReservationEngine engineTrack2 = new FlightReservationEngine();

        Runnable bookingTask = () -> {
            // Check-Then-Act Flaw: Individually synchronized methods leave a dangerous gap between them!
            if (engineTrack2.isSeatAvailable()) {
                System.out.format("[%s] Seat is open! Processing reservation payment...\n", Thread.currentThread().getName());
                try {
                    Thread.sleep(100); // Simulate network payment gateway hop latency
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                engineTrack2.reserveSeat();
                System.out.format("[%s] Reservation confirmed successfully.\n", Thread.currentThread().getName());
            } else {
                System.out.format("[%s] Reservation rejected! Flight sold out.\n", Thread.currentThread().getName());
            }
        };

        Thread passengerAlpha = new Thread(bookingTask, "Passenger-Alpha");
        Thread passengerBeta  = new Thread(bookingTask, "Passenger-Beta");
        passengerAlpha.start(); passengerBeta.start();
        passengerAlpha.join();  passengerBeta.join();

        System.out.println("\nExpected final available seats: 0");
        System.out.format("ACTUAL final available seats  : %d\n", engineTrack2.getAvailableSeats());
        if (engineTrack2.getAvailableSeats() < 0) {
            System.out.println("CRITICAL BUSINESS FAILURE: The aircraft has been OVERBOOKED despite synchronized code!");
        }
    }
}