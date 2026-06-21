package com.concurrency.b_mutual_exclusion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class BankAccount {
    // Shared, global balance across all transactional threads
    private static double balance = 0.0;

    // A 'static synchronized' method locks the entire BankAccount class.
    // It forces incoming transactions into a neat, single-file line.
    public static synchronized void deposit(double amount) {
        balance += amount;
    }

    public static double getBalance() {
        return balance;
    }
}

public class E_SynchronizedMethod {
    public static void main(String[] args) {
        int transactionsPerSystem = 10_000_000;
        double depositAmount = 1.0; // Depositing $1 at a time

        // Utilizing try-with-resources to manage execution safely
        try (ExecutorService transactionProcessor = Executors.newFixedThreadPool(8)) {

            // System 1: Processing 10 million direct deposit micro-transactions
            transactionProcessor.submit(() -> {
                for (int i = 0; i < transactionsPerSystem; i++) {
                    BankAccount.deposit(depositAmount);
                }
            });

            // System 2: Simultaneously processing 10 million mobile payment transactions
            transactionProcessor.submit(() -> {
                for (int i = 0; i < transactionsPerSystem; i++) {
                    BankAccount.deposit(depositAmount);
                }
            });

        } // The pool auto-closes here, waiting until all 20 million deposits are fully processed.

        // --- Financial Audit Output ---
        double expectedBalance = transactionsPerSystem * depositAmount * 2;

        System.out.println("====== BANK ACCOUNT AUDIT ======");
        System.out.printf("Expected Balance: $%,.2f%n", expectedBalance);
        System.out.printf("Actual Balance:   $%,.2f%n", BankAccount.getBalance());

        if (BankAccount.getBalance() == expectedBalance) {
            System.out.println("\nAUDIT PASSED: 'static synchronized' successfully prevented financial data race!");
        } else {
            System.out.println("\nAUDIT FAILED: Balance is corrupted.");
        }
    }
}
