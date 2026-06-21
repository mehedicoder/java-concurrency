package com.concurrency.g_structured;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class TravelAggregatorService {

    // Internal collaborative services decoupled for test injection
    private final FlightService flightService;
    private final HotelService hotelService;

    public TravelAggregatorService(FlightService flightService, HotelService hotelService) {
        this.flightService = flightService;
        this.hotelService = hotelService;
    }

    record TravelPackage(double flightPrice, double hotelPrice, double totalPrice) {}

    /**
     * Aggregates booking details across disparate services using structured containment.
     * Throws an exception if any individual subtask dependencies break.
     */
    public TravelPackage compilePackage(String destination, Duration timeoutSLALimit) throws Exception {

        // Structured scope using Joiner ensuring complete collective success
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.allSuccessfulOrThrow(),
                cfg -> cfg.withTimeout(timeoutSLALimit).withName("travel-aggregator"))) {

            // Forking off dependencies automatically into scoped virtual threads
            Subtask<Double> flightSubtask = scope.fork(() -> flightService.fetchFlightCost(destination));
            Subtask<Double> hotelSubtask = scope.fork(() -> hotelService.fetchHotelCost(destination));

            // Synchronization barrier
            scope.join();

            double flightCost = flightSubtask.get();
            double hotelCost = hotelSubtask.get();

            return new TravelPackage(flightCost, hotelCost, flightCost + hotelCost);

        } catch (StructuredTaskScope.FailedException e) {
            // Unpack and rethrow the core domain error so callers can process it
            if (e.getCause() instanceof Exception domainExc) {
                throw domainExc;
            }
            throw new RuntimeException("Unexpected error during structured aggregation processing", e);
        }
    }
}

// Low-level Interfaces to allow clean mocking or stubbing inside test suites
interface FlightService { double fetchFlightCost(String dest) throws Exception; }
interface HotelService { double fetchHotelCost(String dest) throws Exception; }