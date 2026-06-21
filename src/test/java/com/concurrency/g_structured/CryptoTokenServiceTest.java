package com.concurrency.g_structured;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import static org.junit.jupiter.api.Assertions.*;

class CryptoTokenServiceTest {

    @Test
    void testSuccessfulPlatformTokenGeneration() throws Exception {
        // Arrange
        HashEngine fakeEngine = input -> "HASH-" + input.toUpperCase();
        ThreadFactory testPlatformFactory = Thread.ofPlatform().name("test-platform-", 1).factory();

        CryptoTokenService service = new CryptoTokenService(fakeEngine, testPlatformFactory);

        // Act
        CryptoTokenService.SecurityPayload result = service.generateSecureTokens("salt", Duration.ofSeconds(2));

        // Assert
        assertNotNull(result);
        assertEquals("HASH-SALT-A", result.blockA());
        assertEquals("HASH-SALT-B", result.blockB());
    }

    @Test
    void testPlatformThreadShortCircuitsOnFailure() {
        // Arrange
        HashEngine brokenEngine = input -> {
            throw new IllegalArgumentException("Algorithmic Math Error");
        };
        ThreadFactory testPlatformFactory = Thread.ofPlatform().factory();
        CryptoTokenService service = new CryptoTokenService(brokenEngine, testPlatformFactory);

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            service.generateSecureTokens("salt", Duration.ofSeconds(2));
        });
    }
}