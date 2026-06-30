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

class CryptoTokenServiceTest {

    private static final Duration NORMAL_TIMEOUT = Duration.ofSeconds(3);

    private static ThreadFactory platformThreadFactory() {
        return Thread.ofPlatform()
                .name("crypto-test-platform-", 1)
                .factory();
    }

    @Nested
    @DisplayName("Successful token generation")
    class SuccessfulGeneration {

        @Test
        @DisplayName(
                "Generates both expected token blocks"
        )
        void generatesExpectedSecurityPayload()
                throws Exception {

            // Arrange
            HashEngine engine = input -> "HASH-" + input.toUpperCase();

            B_CryptoTokenService service = new B_CryptoTokenService(engine, platformThreadFactory());

            // Act
            B_CryptoTokenService.SecurityPayload result = service.generateSecureTokens(
                            "salt",
                            NORMAL_TIMEOUT
                    );

            // Assert
            assertAll(
                    () -> assertNotNull(result),
                    () -> assertEquals(
                            "HASH-SALT-A",
                            result.blockA()
                    ),
                    () -> assertEquals(
                            "HASH-SALT-B",
                            result.blockB()
                    )
            );
        }

        @Test
        @DisplayName("Invokes the hash engine with both required inputs")
        void invokesEngineWithBothInputs()
                throws Exception {

            // Arrange
            Set<String> receivedInputs = ConcurrentHashMap.newKeySet();

            HashEngine engine = input -> {
                receivedInputs.add(input);
                return "HASH-" + input;
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            service.generateSecureTokens(
                    "secure-salt",
                    NORMAL_TIMEOUT
            );

            // Assert
            assertEquals(
                    Set.of(
                            "secure-salt-A",
                            "secure-salt-B"
                    ),
                    receivedInputs
            );
        }

        @Test
        @DisplayName("Runs the hash calculations concurrently")
        void runsHashCalculationsConcurrently()
                throws Exception {

            // Arrange
            CountDownLatch bothTasksStarted = new CountDownLatch(2);

            HashEngine engine = input -> {
                bothTasksStarted.countDown();

                boolean bothRunning =
                        bothTasksStarted.await(
                                1,
                                TimeUnit.SECONDS
                        );

                if (!bothRunning) {
                    throw new AssertionError(
                            "Both hash tasks did not run concurrently."
                    );
                }

                return "HASH-" + input;
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            B_CryptoTokenService.SecurityPayload result =
                    service.generateSecureTokens(
                            "salt",
                            NORMAL_TIMEOUT
                    );

            // Assert
            assertAll(
                    () -> assertEquals(
                            "HASH-salt-A",
                            result.blockA()
                    ),
                    () -> assertEquals(
                            "HASH-salt-B",
                            result.blockB()
                    ),
                    () -> assertEquals(
                            0,
                            bothTasksStarted.getCount(),
                            "Both hash tasks should have started."
                    )
            );
        }

        @Test
        @DisplayName("Uses platform threads created by the supplied factory")
        void usesConfiguredPlatformThreads()
                throws Exception {

            // Arrange
            Set<String> threadNames = ConcurrentHashMap.newKeySet();

            AtomicBoolean virtualThreadObserved = new AtomicBoolean(false);

            HashEngine engine = input -> {
                Thread currentThread =
                        Thread.currentThread();

                threadNames.add(currentThread.getName());

                if (currentThread.isVirtual()) {
                    virtualThreadObserved.set(true);
                }

                return "HASH-" + input;
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            service.generateSecureTokens(
                    "salt",
                    NORMAL_TIMEOUT
            );

            // Assert
            assertAll(
                    () -> assertFalse(
                            virtualThreadObserved.get(),
                            "The configured factory should create "
                                    + "platform threads."
                    ),
                    () -> assertEquals(
                            2,
                            threadNames.size(),
                            "Each subtask should use its own thread."
                    ),
                    () -> assertTrue(
                            threadNames.stream().allMatch(
                                    name -> name.startsWith(
                                            "crypto-test-platform-"
                                    )
                            )
                    )
            );
        }
    }

    @Nested
    @DisplayName("Failure handling")
    class FailureHandling {

        @Test
        @DisplayName("Propagates the original hash-engine exception")
        void propagatesOriginalBusinessException() {

            // Arrange
            HashEngine engine = input -> {
                throw new IllegalArgumentException(
                        "Algorithmic Math Error"
                );
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            IllegalArgumentException exception =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> service.generateSecureTokens(
                                    "salt",
                                    NORMAL_TIMEOUT
                            )
                    );

            // Assert
            assertEquals(
                    "Algorithmic Math Error",
                    exception.getMessage()
            );
        }

        @Test
        @DisplayName("Failure of one hash task interrupts the unfinished sibling")
        void failureInterruptsSiblingTask() {

            //The CountDownLatch objects are used only to make the test deterministic and observable
            CountDownLatch blockBStarted = new CountDownLatch(1);

            CountDownLatch blockBInterrupted = new CountDownLatch(1);

            HashEngine engine = input -> {
                if (input.endsWith("-B")) {
                    blockBStarted.countDown();

                    try {
                        /*
                         * Simulates a long-running sibling.
                         */
                        new CountDownLatch(1).await();

                        throw new AssertionError("Block B should have been cancelled.");
                    } catch (InterruptedException exception) {
                        blockBInterrupted.countDown();
                        throw exception;
                    }
                }

                boolean siblingStarted =
                        blockBStarted.await(
                                1,
                                TimeUnit.SECONDS
                        );

                if (!siblingStarted) {
                    throw new AssertionError(
                            "Block B did not start."
                    );
                }

                throw new IllegalStateException(
                        "Block A hash failed"
                );
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            IllegalStateException exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> service.generateSecureTokens(
                                    "salt",
                                    NORMAL_TIMEOUT
                            )
                    );

            // Assert
            assertAll(
                    () -> assertEquals(
                            "Block A hash failed",
                            exception.getMessage()
                    ),
                    () -> assertEquals(
                            0,
                            blockBInterrupted.getCount(),
                            "The unfinished sibling should be interrupted."
                    )
            );
        }
    }

    @Nested
    @DisplayName("Timeout handling")
    class TimeoutHandling {

        @Test
        @DisplayName("Global timeout interrupts both unfinished hash tasks")
        void timeoutInterruptsUnfinishedTasks() {

            // Arrange
            CountDownLatch bothTasksStarted = new CountDownLatch(2);

            CountDownLatch interruptedTasks = new CountDownLatch(2);

            HashEngine engine = input -> {
                bothTasksStarted.countDown();

                try {
                    /*
                     * Both tasks remain blocked until the
                     * structured scope reaches its deadline.
                     * It makes a Task  remain active long enough for the test
                     * to verify automatic sibling cancellation.
                     */
                    new CountDownLatch(1).await();

                    throw new AssertionError(
                            "Hash task should have timed out."
                    );

                } catch (InterruptedException exception) {
                    interruptedTasks.countDown();
                    throw exception;
                }
            };

            B_CryptoTokenService service =
                    new B_CryptoTokenService(
                            engine,
                            platformThreadFactory()
                    );

            // Act
            StructuredTaskScope.TimeoutException exception =
                    assertThrows(
                            StructuredTaskScope.TimeoutException.class,
                            () -> service.generateSecureTokens(
                                    "salt",
                                    Duration.ofMillis(500)
                            )
                    );

            // Assert
            assertAll(
                    () -> assertNotNull(exception),
                    () -> assertEquals(
                            0,
                            bothTasksStarted.getCount(),
                            "Both hash tasks should have started."
                    ),
                    () -> assertEquals(
                            0,
                            interruptedTasks.getCount(),
                            "Both unfinished tasks should be interrupted."
                    )
            );
        }
    }
}
