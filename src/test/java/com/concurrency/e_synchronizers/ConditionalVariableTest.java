package com.concurrency.e_synchronizers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConditionalVariableTest {

    private Field outstandingTransactionsField;

    @BeforeEach
    void setUp() throws NoSuchFieldException {
        // Cache and gain reflective access to the private static internal transactional queue
        outstandingTransactionsField = SettlementProcessor.class.getDeclaredField("outstandingTransactions");
        outstandingTransactionsField.setAccessible(true);
    }

    @Test
    @DisplayName("Scenario 1 (Happy Path): All shards cooperate sequentially to empty the queue completely")
    void testSequentialClearingHappyPath() throws Exception {
        // Arrange: Reset the transaction queue to a clean baseline size
        int totalTestTransactions = 11;
        outstandingTransactionsField.set(null, totalTestTransactions);

        int totalShards = 5;
        List<SettlementProcessor> shardCluster = new ArrayList<>();

        // Act: Initialize and spawn all 5 required shards
        for (int i = 0; i < totalShards; i++) {
            SettlementProcessor shard = new SettlementProcessor(i);
            shardCluster.add(shard);
            shard.start();
        }

        // Wait for all shard threads to process to termination
        for (SettlementProcessor shard : shardCluster) {
            shard.join(3000); // Strict safety timeout cap prevents CI builds from hanging forever if a live-lock occurs
            assertFalse(shard.isAlive(), String.format("%s deadlocked or timed out under sequential wait evaluation.", shard.getName()));
        }

        // Assert: Verify that the global static data structure reached exactly 0
        int finalTransactionsRemaining = outstandingTransactionsField.getInt(null);
        assertEquals(0, finalTransactionsRemaining,
                "The sequencing engine failed to exhaustively process all outstanding transactional data records.");
    }

    @Test
    @DisplayName("Scenario 2 (Edge Case): Engine handles an unexpected thread interrupt signal mid-flight and exits safely")
    void testThreadInterruptionHandlesGracefulShutdown() throws Exception {
        // Arrange: Set a small, manageable queue threshold size
        outstandingTransactionsField.set(null, 20);

        // Spawn a single out-of-turn shard (Shard 4). Because 20 % 5 == 0, Shard 0 must go first.
        // Therefore, Shard 4 will immediately drop into the `sequenceUpdated.await()` blocking loop.
        SettlementProcessor waitingShard = new SettlementProcessor(4);

        // Act
        waitingShard.start();

        // Brief sleep window to ensure the thread actively schedules and drops into the lock condition wait queue
        Thread.sleep(200);

        // Assert thread is still alive and trapped inside the conditional lock guard
        assertTrue(waitingShard.isAlive(), "The shard should be waiting inside the condition gate block.");

        // Forcefully interrupt the waiting thread to trigger the InterruptedException catch block
        waitingShard.interrupt();

        // Join the thread to confirm it drops out cleanly
        waitingShard.join(1000);

        // Assert: Confirm that the thread is dead and handled its internal inter-thread flag state safely
        assertFalse(waitingShard.isAlive(), "The thread failed to break out of the await gate upon receiving an interrupt signal.");
    }
}