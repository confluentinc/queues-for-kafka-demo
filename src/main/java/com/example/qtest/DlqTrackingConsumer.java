package com.example.qtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background reader for the DLQ topic (KIP-1191), feeding the dashboard's DLQ panel.
 * Mirrors InventoryConsumer's standalone-thread-with-start()/stop() shape. Keeps a bounded,
 * in-memory, offset-ordered list of records so the dashboard can show a running count and let
 * a user step through them one at a time, regardless of whether the group copies payloads.
 */
public class DlqTrackingConsumer {

    private static final int MAX_TRACKED_RECORDS = 500;
    private static final String[] CONTEXT_HEADERS = {
        "__dlq.errors.topic",
        "__dlq.errors.partition",
        "__dlq.errors.offset",
        "__dlq.errors.group",
        "__dlq.errors.delivery.count",
        "__dlq.errors.message"
    };

    public static class DlqRecordInfo {
        @JsonProperty("dlqOffset")
        public long dlqOffset;
        @JsonProperty("originalTopic")
        public String originalTopic;
        @JsonProperty("originalPartition")
        public String originalPartition;
        @JsonProperty("originalOffset")
        public String originalOffset;
        @JsonProperty("group")
        public String group;
        @JsonProperty("deliveryCount")
        public String deliveryCount;
        @JsonProperty("reason")
        public String reason;
        @JsonProperty("hasPayload")
        public boolean hasPayload;
        @JsonProperty("orderId")
        public String orderId;
        @JsonProperty("orderItemCount")
        public int orderItemCount;
        @JsonProperty("timestamp")
        public long timestamp;
    }

    private final Deque<DlqRecordInfo> records = new ArrayDeque<>();
    private final Object recordsLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ObjectMapper mapper = new ObjectMapper();
    private Thread consumerThread;

    public List<DlqRecordInfo> getRecords() {
        synchronized (recordsLock) {
            return new ArrayList<>(records);
        }
    }

    public int getCount() {
        synchronized (recordsLock) {
            return records.size();
        }
    }

    public void start() {
        consumerThread = new Thread(this::run, "dlq-tracking-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
    }

    private void run() {
        String dlqTopic = System.getenv("DLQ_TOPIC");
        if (dlqTopic == null || dlqTopic.isEmpty()) {
            dlqTopic = "dlq.orders-queue";
        }

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-dashboard-tracker-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "dlq-dashboard-tracker");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        String apiKey = System.getenv("CONFLUENT_API_KEY");
        String apiSecret = System.getenv("CONFLUENT_API_SECRET");
        if (apiKey != null && apiSecret != null && !apiKey.isEmpty() && !apiSecret.isEmpty()) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));
        }

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(dlqTopic));
            System.out.printf("💀 DLQ tracking consumer subscribed to '%s'%n", dlqTopic);

            while (running.get()) {
                ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : polled) {
                    track(record);
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown
        } catch (Exception e) {
            System.err.println("⚠️ DLQ tracking consumer error: " + e.getMessage());
        }
    }

    private void track(ConsumerRecord<String, byte[]> record) {
        DlqRecordInfo info = new DlqRecordInfo();
        info.dlqOffset = record.offset();
        info.timestamp = record.timestamp();
        info.originalTopic = headerValue(record, CONTEXT_HEADERS[0]);
        info.originalPartition = headerValue(record, CONTEXT_HEADERS[1]);
        info.originalOffset = headerValue(record, CONTEXT_HEADERS[2]);
        info.group = headerValue(record, CONTEXT_HEADERS[3]);
        info.deliveryCount = headerValue(record, CONTEXT_HEADERS[4]);
        info.reason = headerValue(record, CONTEXT_HEADERS[5]);

        byte[] value = record.value();
        if (value != null && value.length > 0) {
            try {
                Order order = mapper.readValue(value, Order.class);
                info.hasPayload = true;
                info.orderId = order.getOrderId();
                info.orderItemCount = order.getOrderItems() != null ? order.getOrderItems().size() : 0;
            } catch (Exception e) {
                info.hasPayload = true;
                info.orderId = new String(value, StandardCharsets.UTF_8);
            }
        } else {
            info.hasPayload = false;
        }

        synchronized (recordsLock) {
            records.addLast(info);
            while (records.size() > MAX_TRACKED_RECORDS) {
                records.removeFirst();
            }
        }
    }

    private static String headerValue(ConsumerRecord<String, byte[]> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
