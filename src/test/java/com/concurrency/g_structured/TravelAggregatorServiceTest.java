package com.concurrency.g_structured;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;

import static org.junit.jupiter.api.Assertions.*;

class TravelAggregatorServiceTest {

    @Test
    @DisplayName("Verify successful aggregation calculations when all services resolve cleanly")
    void testCompilePackageSuccess() throws Exception {
        // Arrange: Build quick dummy stubs for our external service dependencies
        FlightService mockFlight = dest -> 500.00;
        HotelService mockHotel = dest -> 300.00;

        TravelAggregatorService aggregator = new TravelAggregatorService(mockFlight, mockHotel);

        // Act
        TravelAggregatorService.TravelPackage result =
                aggregator.compilePackage("PARIS", Duration.ofSeconds(2));

        // Assert
        assertNotNull(result, "Aggregated data object was returned null.");
        assertEquals(500.00, result.flightPrice());
        assertEquals(300.00, result.hotelPrice());
        assertEquals(800.00, result.totalPrice(), "Total compilation calculations are inaccurate.");
    }

    @Test
    @DisplayName("Verify that a failure in one subtask instantly short-circuits and bubbles up out of the scope")
    void testCompilePackageShortCircuitOnFailure() {
        // Arrange: Flight succeeds, but Hotel throws an explicit business exception
        FlightService mockFlight = dest -> 400.00;
        HotelService mockHotel = dest -> {
            throw new IllegalStateException("Hotel database connection dropped.");
        };

        TravelAggregatorService aggregator = new TravelAggregatorService(mockFlight, mockHotel);

        // Act & Assert
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> {
            aggregator.compilePackage("TOKYO", Duration.ofSeconds(2));
        }, "The structured scope failed to bubble up the inner subtask domain exception.");

        assertEquals("Hotel database connection dropped.", thrown.getMessage());
    }

    @Test
    @DisplayName("Verify that exceeding the designated SLA duration parameter throws a TimeoutException")
    void testCompilePackageSLAThresholdTimeout() {
        // Arrange: Simulate a sluggish remote service that hangs past the SLA parameters
        FlightService mockFlight = dest -> 350.00;
        HotelService mockHotel = dest -> {
            Thread.sleep(1000); // Exceeds our test execution limit of 200ms defined below
            return 200.00;
        };

        TravelAggregatorService aggregator = new TravelAggregatorService(mockFlight, mockHotel);

        // Act & Assert: The framework must interrupt execution pathways and yield a TimeoutException
        assertThrows(StructuredTaskScope.TimeoutException.class, () -> {
            aggregator.compilePackage("LONDON", Duration.ofMillis(200));
        }, "The global scope failed to throw a TimeoutException when the child task breached SLA limits.");
    }
}