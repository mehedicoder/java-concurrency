package com.concurrency.g_structured;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class LogisticsBrokerTest {

    private static final Duration NORMAL_TIMEOUT =
            Duration.ofSeconds(3);

    private static ThreadFactory virtualThreadFactory() {
        return Thread.ofVirtual()
                .name("test-vthread-", 1)
                .factory();
    }

    @Nested
    @DisplayName("Successful responses")
    class SuccessfulResponses {

        @Test
        @DisplayName(
                "Returns the first successful carrier response"
        )
        void firstSuccessfulCarrierWins()
                throws Exception {

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws InterruptedException {

                    Thread.sleep(800);

                    return new C_LogisticsBroker.DeliveryQuote(
                            "FEDEX",
                            "FEDEX-ROUTE",
                            95.0
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    Thread.sleep(50);

                    return new C_LogisticsBroker.DeliveryQuote(
                            "UPS",
                            "UPS-ROUTE",
                            110.0
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            C_LogisticsBroker.DeliveryQuote winner =
                    broker.fetchFastestSuccessfulQuote(
                            "TRACK-123",
                            NORMAL_TIMEOUT
                    );

            assertAll(
                    () -> assertNotNull(winner),
                    () -> assertEquals(
                            "UPS",
                            winner.carrier()
                    ),
                    () -> assertEquals(
                            "UPS-ROUTE",
                            winner.route()
                    ),
                    () -> assertEquals(
                            110.0,
                            winner.baseQuote(),
                            0.001
                    )
            );
        }

        @Test
        @DisplayName(
                "A carrier failure is tolerated when another carrier succeeds"
        )
        void oneFailureDoesNotPreventAnotherSuccess()
                throws Exception {

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws CarrierUnavailableException {

                    throw new CarrierUnavailableException(
                            "FedEx API unavailable"
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    Thread.sleep(100);

                    return new C_LogisticsBroker.DeliveryQuote(
                            "UPS",
                            "UPS-FALLBACK-ROUTE",
                            120.0
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            C_LogisticsBroker.DeliveryQuote result =
                    broker.fetchFastestSuccessfulQuote(
                            "TRACK-456",
                            NORMAL_TIMEOUT
                    );

            assertAll(
                    () -> assertEquals(
                            "UPS",
                            result.carrier()
                    ),
                    () -> assertEquals(
                            "UPS-FALLBACK-ROUTE",
                            result.route()
                    ),
                    () -> assertEquals(
                            120.0,
                            result.baseQuote(),
                            0.001
                    )
            );
        }

        @Test
        @DisplayName(
                "Both carrier requests receive the tracking code"
        )
        void passesTrackingCodeToBothCarriers()
                throws Exception {

            Set<String> receivedRequests =
                    ConcurrentHashMap.newKeySet();

            CountDownLatch bothCallsStarted =
                    new CountDownLatch(2);

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws InterruptedException {

                    receivedRequests.add(
                            "FEDEX:" + trackingCode
                    );

                    bothCallsStarted.countDown();

                    boolean bothStarted =
                            bothCallsStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!bothStarted) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    return new C_LogisticsBroker.DeliveryQuote(
                            "FEDEX",
                            "FEDEX-ROUTE",
                            95.0
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    receivedRequests.add(
                            "UPS:" + trackingCode
                    );

                    bothCallsStarted.countDown();

                    boolean bothStarted =
                            bothCallsStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!bothStarted) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    Thread.sleep(100);

                    return new C_LogisticsBroker.DeliveryQuote(
                            "UPS",
                            "UPS-ROUTE",
                            110.0
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            broker.fetchFastestSuccessfulQuote(
                    "TRACK-789",
                    NORMAL_TIMEOUT
            );

            assertEquals(
                    Set.of(
                            "FEDEX:TRACK-789",
                            "UPS:TRACK-789"
                    ),
                    receivedRequests
            );
        }
    }

    @Nested
    @DisplayName("Cancellation")
    class Cancellation {

        @Test
        @DisplayName(
                "Cancels and interrupts the slower carrier after a winner succeeds"
        )
        void winnerInterruptsSlowerCarrier()
                throws Exception {

            CountDownLatch fedexStarted =
                    new CountDownLatch(1);

            CountDownLatch fedexInterrupted =
                    new CountDownLatch(1);

            CountDownLatch neverReleased =
                    new CountDownLatch(1);

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws InterruptedException {

                    fedexStarted.countDown();

                    try {
                        neverReleased.await();

                        throw new AssertionError(
                                "FedEx should not complete normally"
                        );

                    } catch (InterruptedException exception) {
                        fedexInterrupted.countDown();
                        throw exception;
                    }
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    boolean started =
                            fedexStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!started) {
                        throw new AssertionError(
                                "FedEx did not start"
                        );
                    }

                    return new C_LogisticsBroker.DeliveryQuote(
                            "UPS",
                            "UPS-WINNER-ROUTE",
                            105.0
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            C_LogisticsBroker.DeliveryQuote result =
                    broker.fetchFastestSuccessfulQuote(
                            "TRACK-123",
                            NORMAL_TIMEOUT
                    );

            assertAll(
                    () -> assertEquals(
                            "UPS",
                            result.carrier()
                    ),
                    () -> assertEquals(
                            "UPS-WINNER-ROUTE",
                            result.route()
                    ),
                    () -> assertEquals(
                            0,
                            fedexInterrupted.getCount(),
                            "The unfinished FedEx request should "
                                    + "have been interrupted"
                    )
            );
        }
    }

    @Nested
    @DisplayName("Failure handling")
    class FailureHandling {

        @Test
        @DisplayName(
                "Fails only after all carrier calls have failed"
        )
        void allCarrierFailuresFailOperation() {

            CountDownLatch bothStarted =
                    new CountDownLatch(2);

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws Exception {

                    bothStarted.countDown();

                    boolean bothCallsStarted =
                            bothStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!bothCallsStarted) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    throw new CarrierUnavailableException(
                            "FedEx API unavailable"
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws Exception {

                    bothStarted.countDown();

                    boolean bothCallsStarted =
                            bothStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!bothCallsStarted) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    throw new CarrierUnavailableException(
                            "UPS API unavailable"
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            Exception exception =
                    assertThrows(
                            Exception.class,
                            () -> broker.fetchFastestSuccessfulQuote(
                                    "TRACK-ERROR",
                                    NORMAL_TIMEOUT
                            )
                    );

            assertAll(
                    () -> assertInstanceOf(
                            CarrierUnavailableException.class,
                            exception
                    ),
                    () -> assertTrue(
                            Set.of(
                                    "FedEx API unavailable",
                                    "UPS API unavailable"
                            ).contains(exception.getMessage()),
                            "The propagated failure should come from "
                                    + "one of the carrier calls"
                    )
            );
        }
    }

    @Nested
    @DisplayName("Timeout handling")
    class TimeoutHandling {

        @Test
        @DisplayName(
                "Timeout interrupts all unfinished carrier calls"
        )
        void timeoutInterruptsBothCarrierCalls() {

            CountDownLatch bothStarted =
                    new CountDownLatch(2);

            CountDownLatch interruptedCalls =
                    new CountDownLatch(2);

            CountDownLatch neverReleased =
                    new CountDownLatch(1);

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws InterruptedException {

                    return blockUntilInterrupted(
                            bothStarted,
                            interruptedCalls,
                            neverReleased
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    return blockUntilInterrupted(
                            bothStarted,
                            interruptedCalls,
                            neverReleased
                    );
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            assertThrows(
                    StructuredTaskScope.TimeoutException.class,
                    () -> broker.fetchFastestSuccessfulQuote(
                            "TRACK-TIMEOUT",
                            Duration.ofMillis(300)
                    )
            );

            assertAll(
                    () -> assertEquals(
                            0,
                            bothStarted.getCount(),
                            "Both carrier requests should have started"
                    ),
                    () -> assertEquals(
                            0,
                            interruptedCalls.getCount(),
                            "Both unfinished requests should have "
                                    + "been interrupted"
                    )
            );
        }

        private C_LogisticsBroker.DeliveryQuote
        blockUntilInterrupted(
                CountDownLatch bothStarted,
                CountDownLatch interruptedCalls,
                CountDownLatch neverReleased
        ) throws InterruptedException {

            bothStarted.countDown();

            try {
                neverReleased.await();

                throw new AssertionError(
                        "Carrier request should not complete normally"
                );

            } catch (InterruptedException exception) {
                interruptedCalls.countDown();
                throw exception;
            }
        }
    }

    @Nested
    @DisplayName("Thread configuration")
    class ThreadConfiguration {

        @Test
        @DisplayName(
                "Uses virtual threads from the supplied factory"
        )
        void usesConfiguredVirtualThreads()
                throws Exception {

            AtomicBoolean platformThreadObserved =
                    new AtomicBoolean(false);

            Set<String> threadNames =
                    ConcurrentHashMap.newKeySet();

            CountDownLatch bothStarted =
                    new CountDownLatch(2);

            ApiClient apiClient = new ApiClient() {
                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromFedex(String trackingCode)
                        throws InterruptedException {

                    recordThread();

                    bothStarted.countDown();

                    boolean started =
                            bothStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!started) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    return new C_LogisticsBroker.DeliveryQuote(
                            "FEDEX",
                            "FEDEX-ROUTE",
                            95.0
                    );
                }

                @Override
                public C_LogisticsBroker.DeliveryQuote
                fetchRouteFromUps(String trackingCode)
                        throws InterruptedException {

                    recordThread();

                    bothStarted.countDown();

                    boolean started =
                            bothStarted.await(
                                    1,
                                    TimeUnit.SECONDS
                            );

                    if (!started) {
                        throw new AssertionError(
                                "Both carrier calls did not start"
                        );
                    }

                    Thread.sleep(100);

                    return new C_LogisticsBroker.DeliveryQuote(
                            "UPS",
                            "UPS-ROUTE",
                            110.0
                    );
                }

                private void recordThread() {
                    Thread currentThread =
                            Thread.currentThread();

                    threadNames.add(
                            currentThread.getName()
                    );

                    if (!currentThread.isVirtual()) {
                        platformThreadObserved.set(true);
                    }
                }
            };

            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            apiClient,
                            virtualThreadFactory()
                    );

            broker.fetchFastestSuccessfulQuote(
                    "TRACK-THREAD",
                    NORMAL_TIMEOUT
            );

            assertAll(
                    () -> assertFalse(
                            platformThreadObserved.get(),
                            "Carrier requests should run on "
                                    + "virtual threads"
                    ),
                    () -> assertEquals(
                            2,
                            threadNames.size()
                    ),
                    () -> assertTrue(
                            threadNames.stream().allMatch(
                                    name -> name.startsWith(
                                            "test-vthread-"
                                    )
                            )
                    )
            );
        }
    }

    @Nested
    @DisplayName("Validation")
    class Validation {

        @Test
        @DisplayName(
                "Rejects a null tracking code"
        )
        void rejectsNullTrackingCode() {
            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            successfulApiClient(),
                            virtualThreadFactory()
                    );

            assertThrows(
                    NullPointerException.class,
                    () -> broker.fetchFastestSuccessfulQuote(
                            null,
                            NORMAL_TIMEOUT
                    )
            );
        }

        @Test
        @DisplayName(
                "Rejects a null timeout"
        )
        void rejectsNullTimeout() {
            C_LogisticsBroker broker =
                    new C_LogisticsBroker(
                            successfulApiClient(),
                            virtualThreadFactory()
                    );

            assertThrows(
                    NullPointerException.class,
                    () -> broker.fetchFastestSuccessfulQuote(
                            "TRACK-123",
                            null
                    )
            );
        }

        @Test
        @DisplayName(
                "Rejects a null API client"
        )
        void rejectsNullApiClient() {
            assertThrows(
                    NullPointerException.class,
                    () -> new C_LogisticsBroker(
                            null,
                            virtualThreadFactory()
                    )
            );
        }

        @Test
        @DisplayName(
                "Rejects a null thread factory"
        )
        void rejectsNullThreadFactory() {
            assertThrows(
                    NullPointerException.class,
                    () -> new C_LogisticsBroker(
                            successfulApiClient(),
                            null
                    )
            );
        }
    }

    private static ApiClient successfulApiClient() {
        return new ApiClient() {
            @Override
            public C_LogisticsBroker.DeliveryQuote
            fetchRouteFromFedex(String trackingCode) {

                return new C_LogisticsBroker.DeliveryQuote(
                        "FEDEX",
                        "FEDEX-ROUTE",
                        95.0
                );
            }

            @Override
            public C_LogisticsBroker.DeliveryQuote
            fetchRouteFromUps(String trackingCode) {

                return new C_LogisticsBroker.DeliveryQuote(
                        "UPS",
                        "UPS-ROUTE",
                        110.0
                );
            }
        };
    }

    private static final class CarrierUnavailableException
            extends Exception {

        private CarrierUnavailableException(
                String message
        ) {
            super(message);
        }
    }
}