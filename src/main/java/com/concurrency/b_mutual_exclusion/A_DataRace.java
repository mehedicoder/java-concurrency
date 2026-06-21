package com.concurrency.b_mutual_exclusion;

/**
 * Real-time high-frequency sensor streaming from an Industrial Delta Robot.
 * Two threads concurrently log telemetry packets to a shared un-synchronized primitive counter.
 */
class TelemetryStreamer extends Thread {

    // Shared mutable state prone to data races (Read-Modify-Write collisions)
    // FIX: To eliminate data loss and packet dropouts, change this to an AtomicInteger:
    // static java.util.concurrent.atomic.AtomicInteger totalTelemetryPackets = new java.util.concurrent.atomic.AtomicInteger(0);
    static int totalTelemetryPackets = 0;

    public TelemetryStreamer(String pipelineName) {
        super(pipelineName);
    }

    @Override
    public void run() {
        System.out.format("[%s] Telemetry pipeline online. Streaming high-frequency sensor batches...\n",
                this.getName());

        // High-frequency telemetry pipeline simulating a massive throughput of incoming data packets
        for (int i = 0; i < 10_000_000; i++) {
            // Unsafe concurrent mutation on a shared global primitive
            totalTelemetryPackets++;
        }

        System.out.format("[%s] Batch stream transmission finalized.\n", this.getName());
    }
}

public class A_DataRace {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Robot-System] Initializing parallel telemetry data ingestion...");

        // Create two separate streaming pipelines capturing metrics from different parts of the physical hardware
        Thread actuatorPipeline = new TelemetryStreamer("Telemetry-Pipeline-Actuator");
        Thread bearingWearPipeline = new TelemetryStreamer("Telemetry-Pipeline-BearingWear");

        long startTime = System.currentTimeMillis();

        // Fire off both streams concurrently
        actuatorPipeline.start();
        bearingWearPipeline.start();

        // Wait for both ingestion streams to complete their work
        actuatorPipeline.join();
        bearingWearPipeline.join();

        long endTime = System.currentTimeMillis();
        double elapsedTime = (endTime - startTime) / 1000.0;

        // --- Hardware Metrics Report ---
        System.out.println("\n================ TELEMETRY INTEGRITY REPORT ================");
        System.out.println("Expected Packets: 20,000,000");
        System.out.println("Captured Packets: " + String.format("%,d", TelemetryStreamer.totalTelemetryPackets));
        System.out.format("Ingestion Time:   %.3f seconds\n", elapsedTime);

        // Logical evaluation demonstrating why the data race is a major production failure
        if (TelemetryStreamer.totalTelemetryPackets < 20_000_000) {
            int droppedPackets = 20_000_000 - TelemetryStreamer.totalTelemetryPackets;
            System.out.format("CRITICAL ERROR: Data race triggered! Lost %s telemetry metrics due to pipeline overlap.\n",
                    String.format("%,d", droppedPackets));
        } else {
            System.out.println("SUCCESS: No data loss captured (highly unlikely for 20 million unsynchronized iterations).");
        }
    }
}