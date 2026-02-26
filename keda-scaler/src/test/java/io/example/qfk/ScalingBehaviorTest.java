package io.example.qfk;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Verifies end-to-end scaling behavior as KEDA would observe it.
 *
 * KEDA's Prometheus scaler applies:
 *   desired_replicas = ceil(queryResult / threshold)
 *
 * These tests verify that the exporter produces the correct Prometheus output
 * and that the KEDA scaling formula yields expected replica counts.
 */
@ExtendWith(MockitoExtension.class)
class ScalingBehaviorTest {

    private static final String SHARE_GROUP = "chefs-share-group";
    private static final String TOPIC = "orders-queue";

    @Mock
    private AdminLagClient lagClient;

    private PrometheusRegistry registry;
    private HTTPServer httpServer;
    private QfkLagExporter exporter;

    @BeforeEach
    void setUp() throws IOException {
        registry = new PrometheusRegistry();
        exporter = new QfkLagExporter(lagClient, SHARE_GROUP, TOPIC, registry);
        httpServer = HTTPServer.builder()
                .port(0)
                .registry(registry)
                .buildAndStart();
    }

    @AfterEach
    void tearDown() {
        if (httpServer != null) httpServer.close();
        if (exporter != null) exporter.close();
    }

    /**
     * Polls the exporter (to update the cached gauge) then scrapes /metrics
     * and extracts the qfk_share_group_backlog value.
     */
    private double scrapeBacklogValue() throws Exception {
        exporter.pollNow();
        int port = httpServer.getPort();
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/metrics"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        // Parse the metric value from Prometheus text format
        Pattern pattern = Pattern.compile(
                "qfk_share_group_backlog\\{share_group=\"" + Pattern.quote(SHARE_GROUP) + "\"} (\\S+)");
        Matcher matcher = pattern.matcher(response.body());
        assertTrue(matcher.find(), "Should find qfk_share_group_backlog metric in output:\n" + response.body());
        return Double.parseDouble(matcher.group(1));
    }

    /**
     * Simulates KEDA's scaling formula:
     *   desired = ceil(metricValue / threshold)
     */
    private static int kedaDesiredReplicas(long backlog, long threshold) {
        if (backlog == 0) return 0;
        return (int) Math.ceil((double) backlog / threshold);
    }

    // --- Parameterized scaling formula tests ---

    @ParameterizedTest(name = "backlog={0}, threshold={1} → desired={2}")
    @CsvSource({
            "0,     10, 0",    // no work → scale to zero (KEDA uses minReplicaCount)
            "1,     10, 1",    // tiny backlog → 1 replica
            "10,    10, 1",    // exactly target → 1 replica
            "11,    10, 2",    // just over target → 2 replicas
            "20,    10, 2",    // exactly 2x target → 2 replicas
            "25,    10, 3",    // ceil(25/10) = 3
            "100,   10, 10",   // 10x target → 10 replicas
            "1,      1, 1",    // threshold=1, backlog=1 → 1
            "5,      1, 5",    // threshold=1, backlog=5 → 5 (one replica per message)
            "50,     5, 10",   // threshold=5, backlog=50 → 10
            "7,      5, 2",    // ceil(7/5) = 2
    })
    void scalingFormula(long backlog, long threshold, int expectedReplicas) {
        int desired = kedaDesiredReplicas(backlog, threshold);

        assertEquals(expectedReplicas, desired,
                String.format("ceil(%d / %d) should be %d", backlog, threshold, expectedReplicas));
    }

    // --- Prometheus metric output tests ---

    @Test
    void prometheusMetricReflectsBacklog() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(42L);

        double value = scrapeBacklogValue();

        assertEquals(42.0, value, 0.001, "Prometheus metric should match backlog");
    }

    @Test
    void prometheusMetricUpdatesOnSubsequentScrapes() throws Exception {
        // First scrape: low backlog
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(5L);
        assertEquals(5.0, scrapeBacklogValue(), 0.001);

        // Second scrape: backlog spikes
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(50L);
        assertEquals(50.0, scrapeBacklogValue(), 0.001);

        // Third scrape: backlog drops to zero
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(0L);
        assertEquals(0.0, scrapeBacklogValue(), 0.001);
    }

    @Test
    void kedaScalingFromPrometheusMetric() throws Exception {
        long threshold = 10;

        // Simulate KEDA reading metric and computing replicas
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(25L);
        double metricValue = scrapeBacklogValue();
        int desired = kedaDesiredReplicas((long) metricValue, threshold);
        assertEquals(3, desired, "ceil(25/10) = 3 replicas");

        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(100L);
        metricValue = scrapeBacklogValue();
        desired = kedaDesiredReplicas((long) metricValue, threshold);
        assertEquals(10, desired, "ceil(100/10) = 10 replicas");
    }

    @Test
    void scalesDownWhenBacklogShrinks() throws Exception {
        long threshold = 10;

        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(80L);
        assertEquals(8, kedaDesiredReplicas((long) scrapeBacklogValue(), threshold));

        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(15L);
        assertEquals(2, kedaDesiredReplicas((long) scrapeBacklogValue(), threshold));

        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(0L);
        assertEquals(0, kedaDesiredReplicas((long) scrapeBacklogValue(), threshold));
    }
}
