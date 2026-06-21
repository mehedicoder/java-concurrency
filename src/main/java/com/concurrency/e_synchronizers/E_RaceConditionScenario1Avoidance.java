package com.concurrency.e_synchronizers;

class FlightReservationEngineWithCompoundAtomicity {
    private int totalBookings = 0;
    private int availableSeats = 1; // Only ONE seat left for the flight!

    // =========================================================================
    // 1. WITHOUT SYNCHRONIZATION: Low-level Data Race Condition
    // =========================================================================
    public synchronized void incrementBookingCountSynchronized() {
        // Lost updates will not occur here because the 3 CPU steps are completely protected
        totalBookings++;
    }

    public synchronized boolean reserveSeat() {
        // compound atomicity is guaranteed here because the check-then-act sequence is protected as a single unit of work.
        int current = getAvailableSeats();
        if (current > 0) { //check
            availableSeats--; //act
            return true;
        } else {
            return false;
        }
    }

    // Getters for test inspection
    public int getTotalBookings() { return totalBookings; }
    public int getAvailableSeats() { return availableSeats; }
}

public class E_RaceConditionScenario1Avoidance {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== TRACK 1: Low-Level Race Condition (No Synchronization) ===");
        FlightReservationEngineWithCompoundAtomicity engineTrack1 = new FlightReservationEngineWithCompoundAtomicity();

        Runnable rawCountTask = () -> {
            for (int i = 0; i < 10000; i++) {
                engineTrack1.incrementBookingCountSynchronized();
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
        FlightReservationEngineWithCompoundAtomicity engineTrack2 = new FlightReservationEngineWithCompoundAtomicity();

        Runnable bookingTask = () -> {
            if (engineTrack2.reserveSeat()) {
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