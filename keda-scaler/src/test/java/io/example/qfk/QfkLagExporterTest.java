package io.example.qfk;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Tests the Prometheus lag exporter by starting an HTTP server on a random port,
 * scraping /metrics, and verifying the gauge output.
 */
@ExtendWith(MockitoExtension.class)
class QfkLagExporterTest {

    private static final String SHARE_GROUP = "test-share-group";
    private static final String TOPIC = "test-topic";

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
                .port(0) // random available port
                .registry(registry)
                .buildAndStart();
    }

    @AfterEach
    void tearDown() {
        if (httpServer != null) httpServer.close();
        if (exporter != null) exporter.close();
    }

    private String scrapeMetrics() throws Exception {
        int port = httpServer.getPort();
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/metrics"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    @Test
    void exposesBacklogAsGauge() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(42L);
        exporter.pollNow();

        String metrics = scrapeMetrics();

        assertTrue(metrics.contains("qfk_share_group_backlog"), "Should contain metric name");
        assertTrue(metrics.contains("share_group=\"test-share-group\""), "Should contain label");
        assertTrue(metrics.contains("42.0"), "Should contain backlog value");
    }

    @Test
    void reportsZeroBacklog() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(0L);
        exporter.pollNow();

        String metrics = scrapeMetrics();

        assertTrue(metrics.contains("qfk_share_group_backlog"));
        assertTrue(metrics.contains("0.0"), "Should contain zero backlog");
    }

    @Test
    void reportsLargeBacklog() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(1_000_000L);
        exporter.pollNow();

        String metrics = scrapeMetrics();

        assertTrue(metrics.contains("1000000.0") || metrics.contains("1.0E6"),
                "Should contain large backlog value");
    }

    @Test
    void includesCorrectShareGroupLabel() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(5L);
        exporter.pollNow();

        String metrics = scrapeMetrics();

        // Verify the full metric line format
        assertTrue(metrics.contains("share_group=\"test-share-group\""),
                "Metric should include share_group label with correct value");
    }

    @Test
    void includesHelpAndTypeMetadata() throws Exception {
        when(lagClient.computeBacklog(SHARE_GROUP, TOPIC)).thenReturn(1L);
        exporter.pollNow();

        String metrics = scrapeMetrics();

        assertTrue(metrics.contains("# HELP qfk_share_group_backlog"),
                "Should include HELP metadata");
        assertTrue(metrics.contains("# TYPE qfk_share_group_backlog gauge"),
                "Should be declared as gauge type");
    }
}
