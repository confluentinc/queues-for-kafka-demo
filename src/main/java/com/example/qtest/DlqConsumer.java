package com.example.qtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Reads the dead-letter queue topic that a share group's rejected/expired orders land on
 * (KIP-1191). Works unmodified whether the group is configured to copy the original record
 * (errors.deadletterqueue.copy.record.enable=true) or not: when no payload was copied, this
 * just reports the DLQ context headers; when a payload is present, it also deserializes and
 * prints the original order.
 */
public class DlqConsumer {

    private static final String[] CONTEXT_HEADERS = {
        "__dlq.errors.topic",
        "__dlq.errors.partition",
        "__dlq.errors.offset",
        "__dlq.errors.group",
        "__dlq.errors.delivery.count",
        "__dlq.errors.message"
    };

    public static void main(String[] args) {
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-reader-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "dlq-reader");
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

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║           💀  DLQ Reader                   ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  Topic: %-33s ║%n", dlqTopic);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutdown signal received, stopping DLQ reader...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));

            consumer.subscribe(Collections.singletonList(dlqTopic));
            System.out.printf("✅ Subscribed to '%s', waiting for dead-lettered orders...%n%n", dlqTopic);

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    printDlqRecord(record, mapper);
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("DLQ reader closed.");
    }

    private static void printDlqRecord(ConsumerRecord<String, byte[]> record, ObjectMapper mapper) {
        System.out.printf("💀 DLQ record at offset %d%n", record.offset());
        System.out.println("   Context:");
        for (String headerName : CONTEXT_HEADERS) {
            Header header = record.headers().lastHeader(headerName);
            String value = header == null ? "(not set)" : new String(header.value(), StandardCharsets.UTF_8);
            System.out.printf("     %-28s %s%n", headerName, value);
        }

        byte[] value = record.value();
        if (value == null || value.length == 0) {
            System.out.println("   Payload: none (group has errors.deadletterqueue.copy.record.enable=false — "
                + "headers only)");
        } else {
            try {
                Order order = mapper.readValue(value, Order.class);
                System.out.println("   Payload: original order copied (errors.deadletterqueue.copy.record.enable=true)");
                System.out.printf("     orderId=%s items=%d%n", order.getOrderId(),
                    order.getOrderItems() != null ? order.getOrderItems().size() : 0);
            } catch (Exception e) {
                System.out.println("   Payload: " + new String(value, StandardCharsets.UTF_8));
            }
        }
        System.out.println();
    }
}
