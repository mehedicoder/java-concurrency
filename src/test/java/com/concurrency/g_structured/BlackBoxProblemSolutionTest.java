package com.concurrency.g_structured;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BlackBoxProblemSolutionTest {

    private A_BlackBoxProblemSolution controller;

    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOutput;

    @BeforeEach
    void setUp() {
        controller = new A_BlackBoxProblemSolution();

        originalOut = System.out;
        capturedOutput = new ByteArrayOutputStream();

        System.setOut(
                new PrintStream(
                        capturedOutput,
                        true,
                        StandardCharsets.UTF_8
                )
        );
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    @DisplayName(
            "Inventory failure cancels and interrupts the sibling payment task"
    )
    void testInventoryFailureCancelsPaymentTask() {
        // Arrange
        long startedAt = System.nanoTime();

        // Act
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> controller.handleWebRequest(
                        "User_99",
                        "ORD-11"
                )
        );

        Duration duration =
                Duration.ofNanos(
                        System.nanoTime() - startedAt
                );

        Throwable cause = rootCause(exception);
        String logs = output();

        // Assert
        assertAll(
                () -> assertEquals(
                        "InventoryServiceException",
                        cause.getClass().getSimpleName()
                ),
                () -> assertEquals(
                        "Inventory service unavailable for order ORD-11",
                        cause.getMessage()
                ),
                () -> assertTrue(
                        duration.compareTo(
                                Duration.ofSeconds(3)
                        ) < 0,
                        "The request should fail shortly after the "
                                + "one-second inventory failure."
                ),
                () -> assertTrue(
                        logs.contains(
                                "Payment task was cancelled and interrupted "
                                        + "for user=User_99"
                        )
                ),
                () -> assertFalse(
                        logs.contains(
                                "Credit card charged for user=User_99"
                        ),
                        "Payment must not complete after sibling failure."
                )
        );
    }

    @Test
    @DisplayName(
            "Structured request does not leave a payment child running after returning"
    )
    void testNoChildTaskContinuesAfterRequestReturns()
            throws InterruptedException {

        // Act
        assertThrows(
                RuntimeException.class,
                () -> controller.handleWebRequest(
                        "User_99",
                        "ORD-11"
                )
        );

        String outputWhenRequestReturned =
                output();

        /*
         * Wait longer than necessary to verify that no delayed
         * payment completion appears after the request has exited.
         */
        Thread.sleep(1_500);

        String outputAfterWaiting =
                output();

        // Assert
        assertAll(
                () -> assertTrue(
                        outputWhenRequestReturned.contains(
                                "Payment task was cancelled and interrupted"
                        )
                ),
                () -> assertFalse(
                        outputAfterWaiting.contains(
                                "Credit card charged for user=User_99"
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Web request can run on a virtual thread"
    )
    void testRequestRunsOnVirtualThread()
            throws Exception {

        // Arrange
        try (ExecutorService requestExecutor = Executors.newVirtualThreadPerTaskExecutor()) {

            Future<Boolean> request =
                    requestExecutor.submit(
                            () -> {
                                boolean virtualThread =
                                        Thread.currentThread()
                                                .isVirtual();

                                try {
                                    controller.handleWebRequest(
                                            "User_99",
                                            "ORD-11"
                                    );
                                } catch (RuntimeException exception) {
                                    // Expected inventory failure.
                                }

                                return virtualThread;
                            }
                    );

            // Act
            boolean executedOnVirtualThread = request.get(3,TimeUnit.SECONDS);

            // Assert
            assertTrue(executedOnVirtualThread,
                    "The simulated web request should run "
                            + "on a virtual thread."
            );
        }
    }

    @Test
    @DisplayName(
            "Inventory and payment structured subtasks start before failure propagation"
    )
    void testBothStructuredSubtasksStart() {
        // Act
        assertThrows(
                RuntimeException.class,
                () -> controller.handleWebRequest(
                        "User_99",
                        "ORD-11"
                )
        );

        String logs = output();

        // Assert
        assertAll(
                () -> assertTrue(
                        logs.contains(
                                "Inventory check started for order=ORD-11"
                        )
                ),
                () -> assertTrue(
                        logs.contains(
                                "Payment processing started for user=User_99"
                        )
                ),
                () -> assertTrue(
                        logs.contains(
                                "Inventory database failure for order=ORD-11"
                        )
                )
        );
    }

    private String output() {
        return capturedOutput.toString(
                StandardCharsets.UTF_8
        );
    }

    private static Throwable rootCause(
            Throwable throwable
    ) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }
}