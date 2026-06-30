package com.concurrency.g_structured;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BlackBoxProblemTest {
    private A_BlackBoxProblem controller;
    private PrintStream originalOut;

    private ByteArrayOutputStream capturedOutput;

    @BeforeEach
    void setUp() {
        controller = new A_BlackBoxProblem();

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
        /*
         * shutdown() waits for the leaked payment task to finish,
         * preventing worker threads from escaping the test.
         */
        controller.shutdown();

        System.setOut(originalOut);
    }

    @Test
    @DisplayName(
            "Inventory and payment tasks start concurrently"
    )
    void testChildTasksStartConcurrently() {
        // Act
        assertThrows(
                ExecutionException.class,
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
                )
        );
    }

    @Test
    @DisplayName(
            "Inventory failure is propagated through ExecutionException"
    )
    void testInventoryFailureIsPropagated() {
        // Act
        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> controller.handleWebRequest(
                        "User_99",
                        "ORD-11"
                )
        );

        Throwable cause = rootCause(exception);

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
                        output().contains(
                                "Inventory database failure for order=ORD-11"
                        )
                ),
                () -> assertTrue(
                        output().contains(
                                "Request failed because one child task failed"
                        )
                )
        );
    }

    @Test
    @DisplayName(
            "Payment continues after inventory failure in the unstructured implementation"
    )
    void testPaymentContinuesAfterRequestFailure() {
        // Arrange
        long startedAt = System.nanoTime();

        // Act
        assertThrows(
                ExecutionException.class,
                () -> controller.handleWebRequest(
                        "User_99",
                        "ORD-11"
                )
        );

        Duration requestDuration =
                Duration.ofNanos(
                        System.nanoTime() - startedAt
                );

        // Assert immediately after request failure
        assertAll(
                () -> assertTrue(
                        requestDuration.compareTo(
                                Duration.ofSeconds(3)
                        ) < 0,
                        "The request should fail before the five-second "
                                + "payment operation finishes."
                ),
                () -> assertTrue(
                        output().contains(
                                "Is payment task done immediately after "
                                        + "inventory failure? false"
                        )
                ),
                () -> assertFalse(
                        output().contains(
                                "Credit card charged for user=User_99"
                        ),
                        "Payment should still be running immediately "
                                + "after inventory fails."
                )
        );

        /*
         * The HTTP request already failed, but the independent
         * payment Future continues running.
         */
        await()
                .pollInterval(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(12))
                .until(
                        () -> output().contains(
                                "Credit card charged for user=User_99"
                        )
                );

        assertTrue(
                output().contains(
                        "Credit card charged for user=User_99"
                ),
                "This demonstrates the leaked sibling task."
        );
    }
    private String output() {
        //System.out.flush();

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
