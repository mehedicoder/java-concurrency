package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DataRaceTest {

    @Test
    @DisplayName("Should demonstrate a data race where concurrent updates result in telemetry data loss")
    void testDataRaceLoss() throws InterruptedException {
        // Reset the shared global telemetry register before running the test execution
        TelemetryStreamer.totalTelemetryPackets = 0;

        // Instantiate the updated domain-specific pipeline threads
        Thread actuatorPipeline = new TelemetryStreamer("Test-Pipeline-Actuator");
        Thread bearingWearPipeline = new TelemetryStreamer("Test-Pipeline-BearingWear");

        // Fire off both streams concurrently to force read-modify-write data races
        actuatorPipeline.start();
        bearingWearPipeline.start();

        // Wait for both ingestion streams to finish their execution loops
        actuatorPipeline.join();
        bearingWearPipeline.join();

        // Under high-concurrency non-atomic execution, data will be heavily lost.
        // It will fall short of the true theoretical total of 20,000,000 packets.
        int expectedMax = 20_000_000;

        assertTrue(TelemetryStreamer.totalTelemetryPackets < expectedMax,
                "Data race failed to trigger; captured count shouldn't perfectly hit "
                        + String.format("%,d", expectedMax) + " without atomic safety variables.");
    }
}