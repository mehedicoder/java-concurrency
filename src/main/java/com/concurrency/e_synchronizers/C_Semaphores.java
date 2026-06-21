package com.concurrency.e_synchronizers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

class Airplane extends Thread {
    // COUNTING SEMAPHORE: Controls access to the 3 available airport parking gates
    private static final Semaphore gateSemaphore = new Semaphore(3, true); // true enforces FIFO fairness

    // BINARY SEMAPHORE: Controls access to the single, exclusive Fueling Rig (Capacity = 1)
    private static final Semaphore fuelingRigSemaphore = new Semaphore(1, true);

    public Airplane(String flightNumber) {
        super(flightNumber);
    }

    @Override
    public void run() {
        try {
            System.out.format("[%s] Arrived in local airspace. Requesting a landing gate...\n", this.getName());

            // 1. Acquire a Passenger Gate (Counting Semaphore)
            gateSemaphore.acquire(); // Blocks if all 3 gates are occupied
            System.out.format("[%s] Secured Gate. Passengers are disembarking... (Available Gates Left: %d)\n",
                    this.getName(), gateSemaphore.availablePermits());

            Thread.sleep(1000); // Simulate plane emptying and cleaning

            // 2. Route to Fueling Rig (Binary Semaphore)
            System.out.format("[%s] Fuel levels low. Waiting for the exclusive Fueling Rig...\n", this.getName());

            fuelingRigSemaphore.acquire(); // Blocks if another plane is fueling (Mutex behavior)
            System.out.format("[%s] Connected to Fueling Rig. Pumping jet fuel...\n", this.getName());

            Thread.sleep(800); // Simulate dangerous fueling procedure

            System.out.format("[%s] Fueling complete. Disconnecting from rig.\n", this.getName());
            fuelingRigSemaphore.release(); // Free up the fueling rig for the next plane

            // 3. Clear Gate and Take Off
            System.out.format("[%s] Pushing back from gate. Taking off!\n", this.getName());
            gateSemaphore.release(); // Free up the gate for circling planes

        } catch (InterruptedException e) {
            System.out.format("[%s] Flight plan aborted via emergency control tower interrupt.\n", this.getName());
            Thread.currentThread().interrupt();
        }
    }
}

public class C_Semaphores {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Control Tower] Air Traffic Control System activated. 3 Gates Open. 1 Fueling Rig Active.\n");

        // Spawn a heavy burst of 6 airplanes targeting the limited airport infrastructure
        List<Thread> airplanes = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            airplanes.add(new Airplane("Flight-00" + i));
        }

        // Fire off all planes at once
        for (Thread plane : airplanes) {
            plane.start();
        }

        // Wait for all flights to complete their pathing
        for (Thread plane : airplanes) {
            plane.join(5000);
        }

        System.out.println("\n[Control Tower] All flights processed cleanly. Airspace cleared.");
    }
}