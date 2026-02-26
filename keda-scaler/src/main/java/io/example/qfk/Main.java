package io.example.qfk;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Entry point for the QfK Lag Exporter.
 *
 * Reads configuration from environment variables, creates a Kafka AdminClient
 * connected to Confluent Cloud (SASL_SSL + PLAIN), and starts a Prometheus
 * HTTP server that exposes share group backlog as a gauge metric.
 *
 * Prometheus scrapes this endpoint, and KEDA's Prometheus scaler queries
 * Prometheus to determine the desired replica count for QfK share consumers.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // --- Kafka / Confluent Cloud configuration ---
        String bootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String apiKey = requireEnv("KAFKA_API_KEY");
        String apiSecret = requireEnv("KAFKA_API_SECRET");
        String securityProtocol = env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        String saslMechanism = env("KAFKA_SASL_MECHANISM", "PLAIN");

        // --- QfK identifiers ---
        String shareGroupId = requireEnv("QFK_SHARE_GROUP_ID");
        String topicName = env("QFK_TOPIC_NAME", null);

        // --- Metrics server port ---
        int metricsPort = Integer.parseInt(env("METRICS_PORT", "9400"));

        // --- Backlog poll interval ---
        long pollIntervalMs = Long.parseLong(env("BACKLOG_POLL_INTERVAL_MS", "1000"));

        // Build AdminClient properties for Confluent Cloud
        Properties adminProps = new Properties();
        adminProps.setProperty("bootstrap.servers", bootstrapServers);
        adminProps.setProperty("security.protocol", securityProtocol);
        adminProps.setProperty("sasl.mechanism", saslMechanism);
        adminProps.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));

        log.info("Connecting to Confluent Cloud at {} (protocol={}, mechanism={})",
                bootstrapServers, securityProtocol, saslMechanism);
        log.info("Share group: {}, topic filter: {}",
                shareGroupId, topicName != null ? topicName : "(all)");

        // Create components
        AdminLagClient lagClient = new AdminLagClient(adminProps);
        PrometheusRegistry registry = new PrometheusRegistry();
        QfkLagExporter exporter = new QfkLagExporter(lagClient, shareGroupId, topicName, registry, pollIntervalMs);

        // Start Prometheus HTTP server
        HTTPServer httpServer = HTTPServer.builder()
                .port(metricsPort)
                .registry(registry)
                .buildAndStart();

        log.info("QfK Lag Exporter started — Prometheus metrics on port {}", metricsPort);

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            httpServer.close();
            exporter.close();
            lagClient.close();
            log.info("Shutdown complete");
        }));

        // Block until shutdown
        Thread.currentThread().join();
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Required environment variable not set: " + name);
        }
        return value;
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
