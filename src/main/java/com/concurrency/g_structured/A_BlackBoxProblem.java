package com.concurrency.g_structured;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

 /**
 *
 * This is a demonstration of the orchestration problem of independent Future tasks
 * that continue running after one fails and task orchestrator main thread has no idea about it.
 * Black Box Problem here specifically refers to a complete loss of visibility,
 * control, and relationship lifecycle between a parent operation and its asynchronous child task.
 *
 */
public class A_BlackBoxProblem {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public void handleWebRequest(
            String userId,
            String orderId
    ) throws ExecutionException, InterruptedException {

        log(
                "Processing request for user="
                        + userId
                        + ", order="
                        + orderId
        );

        Future<String> inventoryTask = executor.submit(() -> checkInventoryStock(orderId));

        Future<String> paymentTask = executor.submit(() -> chargeCreditCard(userId));

        try {
            /*
             * Inventory fails after one second.
             *
             * inventoryTask.get() throws ExecutionException,
             * so the code never reaches paymentTask.get().
             */
            String inventoryResult =
                    inventoryTask.get();

            String paymentResult =
                    paymentTask.get();

            String receipt =
                    inventoryResult
                            + " | "
                            + paymentResult;

            log("Receipt: " + receipt);

        } catch (ExecutionException exception) {
            /*
             * BLACK-BOX PROBLEM:
             *
             * The inventory task failed, but paymentTask is still
             * running because Future tasks are independent.
             *
             * We intentionally do not cancel paymentTask here
             * to demonstrate the problem.
             */
            log("Request failed because one child task failed: " + rootCause(exception).getMessage());

            //Observe whether the sibling payment task is still running.
            log("Is payment task done immediately after " + "inventory failure? " + paymentTask.isDone());

            throw exception;
        }
    }

    private String checkInventoryStock(String orderId) throws InterruptedException {

        log("Inventory check started for order=" + orderId);

        Thread.sleep(1_000);

        log("Inventory database failure for order=" + orderId);

        throw new InventoryServiceException("Inventory service unavailable for order " + orderId);
    }

    private String chargeCreditCard(String userId) throws InterruptedException {

        log("Payment processing started for user=" + userId);

        /*
         * Payment continues even after inventory has failed.
         */
        Thread.sleep(5_000);

        log("Credit card charged for user=" + userId);

        return "PAID";
    }

    public void shutdown() {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(
                    10,
                    TimeUnit.SECONDS
            )) {
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
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
                "%-30s %s%n",
                "[" + Thread.currentThread().getName() + "]",
                message
        );
    }

    public static void main(String[] args) throws InterruptedException {
        A_BlackBoxProblem controller = new A_BlackBoxProblem();

        Thread aliceRequest =
                new Thread(
                        () -> processRequest(
                                controller,
                                "User_99",
                                "ORD-11"
                        ),
                        "http-nio-8080-exec-1"
                );

        aliceRequest.start();
        aliceRequest.join();

        /*
         * Wait long enough to observe that the payment task
         * continues after the request has failed.
         */
        log(
                "HTTP request thread has finished, "
                        + "but a child task may still be running."
        );

        Thread.sleep(6_000);

        controller.shutdown();

        log("Application finished.");
    }

    private static void processRequest(A_BlackBoxProblem controller, String userId, String orderId) {
        try {
            controller.handleWebRequest(
                    userId,
                    orderId
            );

        } catch (ExecutionException exception) {
            log(
                    "HTTP 500 returned to client: "
                            + rootCause(exception).getMessage()
            );

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            log(
                    "HTTP request interrupted for user="
                            + userId
            );

        } catch (Exception exception) {
            log(
                    "Unexpected request failure: "
                            + exception.getMessage()
            );
        }
    }
}

class InventoryServiceException
        extends RuntimeException {

    InventoryServiceException(String message) {
        super(message);
    }
}