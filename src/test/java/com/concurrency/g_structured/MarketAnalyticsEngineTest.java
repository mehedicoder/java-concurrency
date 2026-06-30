package com.concurrency.g_structured;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import static org.junit.jupiter.api.Assertions.*;

class MarketAnalyticsEngineTest {

    @Test
    @DisplayName("Verify successful end-to-end processing across nested thread models")
    void testGenerateTradeSignalSuccess() throws Exception {
        // Arrange
        MarketDataClient fakeNetworkClient = ticker -> "RAW-DATA-STREAM-100293";
        MathAnalysisEngine fakeMathEngine = data -> "TREND-BULLISH-CONFIRMED-" + data.hashCode();

        ThreadFactory testVFactory = Thread.ofVirtual().name("io-vthread-", 1).factory();
        ThreadFactory testPFactory = Thread.ofPlatform().name("cpu-pthread-", 1).factory();

        D_MarketAnalyticsEngine engine = new D_MarketAnalyticsEngine(
                fakeNetworkClient, fakeMathEngine, testVFactory, testPFactory
        );

        // Act
        D_MarketAnalyticsEngine.MarketSignal signal = engine.generateTradeSignal("BTC", Duration.ofSeconds(2));

        // Assert
        assertNotNull(signal);
        assertEquals("BTC", signal.ticker());
        assertEquals("RAW-DATA-STREAM-100293", signal.dataDump());
        assertTrue(signal.statisticalTrend().startsWith("TREND-BULLISH-CONFIRMED"));
    }

    @Test
    @DisplayName("Verify that if network layers collapse, the downstream CPU execution is safely prevented")
    void testNetworkFailureShortCircuitsWorkflow() {
        // Arrange
        MarketDataClient brokenNetworkClient = ticker -> {
            throw new java.net.ConnectException("Remote exchange api rejected request (HTTP 429)");
        };
        MathAnalysisEngine fakeMathEngine = data -> "SHOULD-NOT-RUN";

        ThreadFactory testVFactory = Thread.ofVirtual().factory();
        ThreadFactory testPFactory = Thread.ofPlatform().factory();

        D_MarketAnalyticsEngine engine = new D_MarketAnalyticsEngine(
                brokenNetworkClient, fakeMathEngine, testVFactory, testPFactory
        );

        // Act & Assert
        assertThrows(java.net.ConnectException.class, () -> {
            engine.generateTradeSignal("ETH", Duration.ofSeconds(2));
        }, "The mixed engine failed to immediately abort when the parent network scope threw an exception.");
    }
}