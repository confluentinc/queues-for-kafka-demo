package io.example.qfk;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdminLagClientTest {

    @Mock
    private AdminClient adminClient;

    private AdminLagClient lagClient;

    @BeforeEach
    void setUp() {
        lagClient = new AdminLagClient(adminClient);
    }

    @Test
    void sumsLagAcrossPartitions() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(100L)),
                new TopicPartition("orders", 1),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(50L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(150L, backlog);
    }

    @Test
    void filtersByTopic() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(100L)),
                new TopicPartition("other-topic", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(999L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(100L, backlog);
    }

    @Test
    void includesAllTopicsWhenFilterIsNull() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(100L)),
                new TopicPartition("other-topic", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(50L))
        ));

        long backlog = lagClient.computeBacklog("my-group", null);

        assertEquals(150L, backlog);
    }

    @Test
    void includesAllTopicsWhenFilterIsEmpty() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(80L)),
                new TopicPartition("other", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(20L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "");

        assertEquals(100L, backlog);
    }

    @Test
    void returnsZeroWhenLagIsEmpty() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.empty())
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(0L, backlog);
    }

    @Test
    void returnsZeroWhenLagIsZero() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(0L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(0L, backlog);
    }

    @Test
    void returnsZeroWhenNoPartitionsExist() throws Exception {
        stubOffsets("my-group", Map.of());

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(0L, backlog);
    }

    @Test
    void ignoresNegativeLag() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(-1L)),
                new TopicPartition("orders", 1),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(30L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(30L, backlog);
    }

    @Test
    void returnsZeroOnExecutionException() throws Exception {
        // Simulate a KafkaFuture that fails with ExecutionException (realistic broker error)
        ListShareGroupOffsetsResult result = mock(ListShareGroupOffsetsResult.class);
        KafkaFuture<Map<TopicPartition, SharePartitionOffsetInfo>> failedFuture =
                mock(KafkaFuture.class);
        when(failedFuture.get()).thenThrow(new ExecutionException("Broker unavailable",
                new RuntimeException("connection refused")));
        when(result.partitionsToOffsetInfo("my-group")).thenReturn(failedFuture);
        when(adminClient.listShareGroupOffsets(anyMap())).thenReturn(result);

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(0L, backlog);
    }

    @Test
    void returnsZeroOnUnexpectedException() throws Exception {
        // Simulate an unexpected RuntimeException (e.g. NPE, serialization error)
        ListShareGroupOffsetsResult result = mock(ListShareGroupOffsetsResult.class);
        when(result.partitionsToOffsetInfo("my-group"))
                .thenThrow(new RuntimeException("Unexpected error"));
        when(adminClient.listShareGroupOffsets(anyMap())).thenReturn(result);

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(0L, backlog);
    }

    @Test
    void handlesMultiplePartitionsForSingleTopic() throws Exception {
        stubOffsets("my-group", Map.of(
                new TopicPartition("orders", 0),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(10L)),
                new TopicPartition("orders", 1),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(20L)),
                new TopicPartition("orders", 2),
                new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.of(30L))
        ));

        long backlog = lagClient.computeBacklog("my-group", "orders");

        assertEquals(60L, backlog);
    }

    private void stubOffsets(String groupId, Map<TopicPartition, SharePartitionOffsetInfo> offsets) {
        ListShareGroupOffsetsResult result = mock(ListShareGroupOffsetsResult.class);
        when(result.partitionsToOffsetInfo(groupId))
                .thenReturn(KafkaFuture.completedFuture(offsets));
        when(adminClient.listShareGroupOffsets(anyMap())).thenReturn(result);
    }
}
