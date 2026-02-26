package io.example.qfk;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus exporter that exposes QfK share group backlog as a gauge.
 *
 * Polls {@link AdminLagClient#computeBacklog} on a configurable interval
 * and caches the result so Prometheus scrapes return instantly without
 * blocking on a Kafka AdminClient call.
 *
 * KEDA's Prometheus scaler queries this metric and applies:
 *   desired_replicas = ceil(backlog / threshold)
 */
public class QfkLagExporter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QfkLagExporter.class);
    static final String METRIC_NAME = "qfk_share_group_backlog";

    private final AdminLagClient lagClient;
    private final String shareGroupId;
    private final String topicName;
    private final Gauge gauge;
    private final ScheduledExecutorService scheduler;

    /**
     * Production constructor — starts a background poller that updates the
     * gauge every {@code pollIntervalMs} milliseconds.
     */
    public QfkLagExporter(AdminLagClient lagClient,
                          String shareGroupId,
                          String topicName,
                          PrometheusRegistry registry,
                          long pollIntervalMs) {
        this.lagClient = lagClient;
        this.shareGroupId = shareGroupId;
        this.topicName = topicName;

        this.gauge = Gauge.builder()
                .name(METRIC_NAME)
                .help("Total share group backlog (queue depth)")
                .labelNames("share_group")
                .register(registry);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "backlog-poller");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::pollNow, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    // Package-private constructor for tests — no background polling.
    // Tests call pollNow() explicitly after configuring mocks.
    QfkLagExporter(AdminLagClient lagClient,
                   String shareGroupId,
                   String topicName,
                   PrometheusRegistry registry) {
        this.lagClient = lagClient;
        this.shareGroupId = shareGroupId;
        this.topicName = topicName;

        this.gauge = Gauge.builder()
                .name(METRIC_NAME)
                .help("Total share group backlog (queue depth)")
                .labelNames("share_group")
                .register(registry);

        this.scheduler = null;
    }

    /**
     * Polls the AdminClient for the current backlog and updates the gauge.
     * Called by the background scheduler in production, or directly in tests.
     */
    void pollNow() {
        try {
            long backlog = lagClient.computeBacklog(shareGroupId, topicName);
            log.info("Poll — share group '{}' backlog: {}", shareGroupId, backlog);
            gauge.labelValues(shareGroupId).set(backlog);
        } catch (Exception e) {
            log.error("Error polling backlog for share group '{}'", shareGroupId, e);
        }
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
}
