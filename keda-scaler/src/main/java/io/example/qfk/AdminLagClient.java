package io.example.qfk;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Wraps the Kafka AdminClient (AK 4.2+) to compute share group backlog.
 *
 * Connects to Confluent Cloud via standard Kafka AdminClient (SASL_SSL + PLAIN).
 * This provides near real-time lag data, which is the recommended approach for
 * autoscaling loops. (The Confluent Cloud Metrics API has ~5-minute delay and
 * is better suited for dashboards / alerting.)
 *
 * "Backlog" at the share-group level represents the total number of records
 * that are available but not yet delivered or acknowledged — conceptually
 * equivalent to "queue depth" in traditional messaging systems.
 */
public class AdminLagClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AdminLagClient.class);

    private final AdminClient adminClient;

    public AdminLagClient(Properties adminProps) {
        this.adminClient = AdminClient.create(adminProps);
        log.info("AdminLagClient created — connected to {}", adminProps.getProperty("bootstrap.servers"));
    }

    // Package-private constructor for testing with a mock AdminClient
    AdminLagClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Compute the total backlog for a share group, optionally filtered by topic.
     *
     * Uses {@code AdminClient#listShareGroupOffsets(...)} from AK 4.2+ to obtain
     * per-partition offset information for the share group, then sums the backlog
     * across all relevant partitions.
     *
     * @param shareGroupId the share group identifier (e.g. "chefs-share-group")
     * @param topicName    optional topic filter; if null, all topics in the group are included
     * @return total backlog (queue depth) across all matching partitions
     */
    public long computeBacklog(String shareGroupId, String topicName) {
        try {
            // Use listShareGroupOffsets to get per-partition state for this share group.
            ListShareGroupOffsetsSpec spec = new ListShareGroupOffsetsSpec();
            ListShareGroupOffsetsResult result = adminClient.listShareGroupOffsets(
                    Map.of(shareGroupId, spec));

            Map<TopicPartition, SharePartitionOffsetInfo> offsets =
                    result.partitionsToOffsetInfo(shareGroupId).get();

            long totalBacklog = 0;
            for (Map.Entry<TopicPartition, SharePartitionOffsetInfo> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();

                // Filter by topic if specified
                if (topicName != null && !topicName.isEmpty() && !tp.topic().equals(topicName)) {
                    continue;
                }

                // SharePartitionOffsetInfo.lag() returns the backlog for this partition.
                long partitionBacklog = entry.getValue().lag().orElse(0L);
                if (partitionBacklog > 0) {
                    totalBacklog += partitionBacklog;
                }
            }

            log.debug("Share group '{}' backlog: {} (topic filter: {})",
                    shareGroupId, totalBacklog, topicName != null ? topicName : "none");

            return totalBacklog;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while computing backlog for share group '{}'", shareGroupId, e);
            return 0;
        } catch (ExecutionException e) {
            log.error("Failed to compute backlog for share group '{}'", shareGroupId, e);
            return 0;
        } catch (Exception e) {
            log.error("Unexpected error computing backlog for share group '{}'", shareGroupId, e);
            return 0;
        }
    }

    @Override
    public void close() {
        log.info("Closing AdminLagClient");
        adminClient.close();
    }
}
