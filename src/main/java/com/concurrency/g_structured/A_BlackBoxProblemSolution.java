package com.concurrency.g_structured;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.StructuredTaskScope;

/**
*
* Demonstration of the Automatic "Short-Circuiting" and Cancellation of StructuredTaskScope.
* Open the black box by enforcing a strict parent-child hierarchy,
* automatic cancellation, and lifetime guarantees.
*
*/
public class A_BlackBoxProblemSolution {
    public static void main(String[] args) {
        A_BlackBoxProblemSolution controller = new A_BlackBoxProblemSolution();

        /*
         * Each submitted web request runs on its own virtual thread.
         */
        try (ExecutorService requestExecutor = Executors.newVirtualThreadPerTaskExecutor()) {

            Future<?> request =
                    requestExecutor.submit(
                            () -> processRequest(
                                    controller,
                                    "User_99",
                                    "ORD-11"
                            )
                    );

            /*
             * Console demonstration only.
             * A real server manages request lifetime itself.
             */
            request.get();

            log(
                    "HTTP request virtual thread finished. "
                            + "No structured child task remains active."
            );

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            log(
                    "Main thread was interrupted while "
                            + "waiting for the request."
            );

        } catch (ExecutionException exception) {
            log(
                    "Virtual request task failed unexpectedly: "
                            + rootCause(exception).getMessage()
            );
        }

        log("Application finished.");
    }


    public void handleWebRequest(
            String userId,
            String orderId
    ) throws InterruptedException {

        log(
                "Processing request for user="
                        + userId
                        + ", order="
                        + orderId
        );

        //Orchestration: StructuredTaskScope.<String>open() by default uses a "Shutdown-on-Failure"
        try (var scope = StructuredTaskScope.<String>open()) {
            StructuredTaskScope.Subtask<String> inventoryTask = scope.fork(() -> checkInventoryStock(orderId));
            StructuredTaskScope.Subtask<String> paymentTask = scope.fork(() -> chargeCreditCard(userId));

            /*
             * Wait for both subtasks as one structured unit.
             *
             * When inventory fails, the scope cancels the
             * unfinished payment task.
             */
            scope.join();

            String receipt = inventoryTask.get() + " | " + paymentTask.get();

            log("Receipt: " + receipt);

        } catch (
                StructuredTaskScope.FailedException exception
        ) {
            Throwable cause =
                    rootCause(exception);

            log(
                    "Structured request failed: "
                            + cause.getMessage()
            );

            throw new WebRequestProcessingException(
                    "Request failed for user="
                            + userId
                            + ", order="
                            + orderId,
                    cause
            );
        }
    }

    private static void processRequest(A_BlackBoxProblemSolution controller, String userId, String orderId) {
        try {
            controller.handleWebRequest(userId, orderId);

        } catch (WebRequestProcessingException exception) {
            log("HTTP 500 returned to client: " + rootCause(exception).getMessage());

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log("HTTP request interrupted for user=" + userId);
        }
    }

    private String checkInventoryStock(
            String orderId
    ) throws InterruptedException {
        log("Inventory check started for order=" + orderId);

        Thread.sleep(1_000);

        log("Inventory database failure for order=" + orderId);

        throw new InventoryServiceException("Inventory service unavailable for order " + orderId);
    }

    private String chargeCreditCard(String userId) throws InterruptedException {
        log("Payment processing started for user=" + userId);
        try {
            Thread.sleep(5_000);
            log("Credit card charged for user=" + userId);
            return "PAID";

        } catch (InterruptedException exception) {
            log("Payment task was cancelled and interrupted " + "for user=" + userId);

            Thread.currentThread().interrupt();
            throw exception;
        }
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;

        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current;
    }

    private static void log(String message) {
        System.out.printf(
                "%-40s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }

    private static final class InventoryServiceException extends RuntimeException {

        private InventoryServiceException(
                String message
        ) {
            super(message);
        }
    }

    private static final class WebRequestProcessingException extends RuntimeException {
        private WebRequestProcessingException(
                String message,
                Throwable cause
        ) {
            super(message, cause);
        }
    }
}