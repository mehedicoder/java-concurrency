package com.concurrency.g_structured;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import static org.junit.jupiter.api.Assertions.*;

class LogisticsBrokerTest {

    @Test
    void testVirtualThreadFirstWinnerSucceeds() throws Exception {
        // Arrange: Fedex takes 800ms, UPS takes 10ms (Fast Winner)
        ApiClient mockClient = new ApiClient() {
            @Override
            public LogisticsBroker.DeliveryConsensus fetchRouteFromFedex(String code) throws Exception {
                Thread.sleep(800);
                return new LogisticsBroker.DeliveryConsensus("FEDEX-ROUTE", 95.0);
            }

            @Override
            public LogisticsBroker.DeliveryConsensus fetchRouteFromUps(String code) throws Exception {
                Thread.sleep(10);
                return new LogisticsBroker.DeliveryConsensus("UPS-ROUTE", 110.0);
            }
        };

        ThreadFactory testVThreadFactory = Thread.ofVirtual().name("test-vthread-", 1).factory();
        LogisticsBroker broker = new LogisticsBroker(mockClient, testVThreadFactory);

        // Act
        LogisticsBroker.DeliveryConsensus winner = broker.fetchFastestConsensus("TRACK-123", Duration.ofSeconds(2));

        // Assert
        assertNotNull(winner);
        assertEquals("UPS-ROUTE", winner.carrierRoute(), "The faster service did not win the consensus.");
        assertEquals(110.0, winner.baseQuote());
    }
}
