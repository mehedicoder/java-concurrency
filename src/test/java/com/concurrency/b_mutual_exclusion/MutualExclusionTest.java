package com.concurrency.b_mutual_exclusion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

class MutualExclusionTest {

    private Field stockCountField;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Access the private static internal counter from WarehouseRobot using reflection
        stockCountField = WarehouseRobot.class.getDeclaredField("totalStockCount");
        stockCountField.setAccessible(true);

        // Reset the static counter to 0 before every test run to prevent test cross-contamination
        stockCountField.set(null, 0);
    }

    @Test
    @DisplayName("Should guarantee that mutual exclusion completely prevents data loss across concurrent worker threads")
    void testMutualExclusionGuaranteesDataIntegrity() throws Exception {
        int itemsPerRobot = 10;

        // Instantiate two parallel warehouse robots competing for the same inventory stock resource
        Thread robotAlpha = new WarehouseRobot("Test-Robot-Alpha", itemsPerRobot);
        Thread robotBeta = new WarehouseRobot("Test-Robot-Beta", itemsPerRobot);

        // Start both concurrent processes simultaneously
        robotAlpha.start();
        robotBeta.start();

        // Await the completion of both threads with a safety timeout (prevents hanging in CI pipelines if a deadlock occurs)
        robotAlpha.join(2000);
        robotBeta.join(2000);

        // Ensure both threads terminated cleanly and did not freeze
        assertFalse(robotAlpha.isAlive(), "Robot-Alpha failed to terminate within the expected timeframe.");
        assertFalse(robotBeta.isAlive(), "Robot-Beta failed to terminate within the expected timeframe.");

        // Read the private state of the field
        int actualStockCount = stockCountField.getInt(null);
        int expectedTotalStock = itemsPerRobot * 2;

        // Assert that the ReentrantLock successfully prevented any race conditions or update dropouts
        assertEquals(expectedTotalStock, actualStockCount,
                String.format("Data loss detected! ReentrantLock failed to protect data integrity. Expected %d but got %d",
                        expectedTotalStock, actualStockCount));
    }
}