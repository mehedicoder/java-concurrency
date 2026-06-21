package com.concurrency.g_structured;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

class UserRequestProcessorTest {

    @Test
    @DisplayName("Verify that child threads inherit ScopedValue data inside a StructuredTaskScope context")
    void testScopedValueInheritanceSuccess() throws Exception {
        // Arrange
        DetailsClient fakeClient = username -> "PROFILE_PAYLOAD_FOR_" + username.toUpperCase();
        UserRequestProcessor processor = new UserRequestProcessor(fakeClient);

        // Act
        String result = processor.processRequest("User_123");

        // Assert
        assertEquals("PROFILE_PAYLOAD_FOR_USER_123", result);
    }

    @Test
    @DisplayName("Verify that when execution exits the dynamic boundary, the ScopedValue is automatically cleaned up")
    void testScopedValueIsUnboundAfterScopeFinishes() throws Exception {
        // Arrange
        DetailsClient instantClient = username -> "OK";
        UserRequestProcessor processor = new UserRequestProcessor(instantClient);

        // Act: Execute transaction block completely to evaluate cleanup
        processor.processRequest("User_777");

        // Assert: Outside of the dynamic 'where' method call block, reading the value must throw a failure
        assertThrows(NoSuchElementException.class, () -> {
            UserRequestProcessor.CONTEXT_USER.get();
        }, "The ScopedValue data leaked outside of its execution boundary blocks.");
    }
}