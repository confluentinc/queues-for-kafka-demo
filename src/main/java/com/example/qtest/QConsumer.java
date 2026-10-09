package com.example.qtest;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal example using KafkaShareConsumer (queue/Share group).
 *
 * Start TWO instances of this program (two terminals) to observe queue-style delivery:
 * each message will be given to only one of the consumers.
 */
public class QConsumer {
    private static final String TOPIC = "orders-queue";
    private static final AtomicBoolean running = new AtomicBoolean(true);

    private static void printHeader(String chefName) {
        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║         🔪  StreamBytes Kitchen           ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  Chef: %-34s ║%n", chefName);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }

    private static void printChef(String chefName) {
        System.out.printf("👨🏻‍🍳 %s listening for new orders...", chefName);
    }

    public static void main(String[] args) {
        String chefName = args.length > 0 ? args[0] : "Unnamed Chef";
        printHeader(chefName);

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        long processingDelayMs = 2000;
        String delayEnv = System.getenv("CHEF_PROCESSING_DELAY_MS");
        if (delayEnv != null && !delayEnv.isEmpty()) {
            try {
                processingDelayMs = Long.parseLong(delayEnv);
                // Ensure minimum delay of 2 seconds for throttling
                if (processingDelayMs < 2000) {
                    System.out.println("⚠️  CHEF_PROCESSING_DELAY_MS is less than 2000ms, enforcing minimum 2000ms for throttling");
                    processingDelayMs = 2000;
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid CHEF_PROCESSING_DELAY_MS, using default 2000ms");
            }
        }

        double acceptRate = 0.80;
        double releaseRate = 0.15;
        String acceptRateEnv = System.getenv("CHEF_ACCEPT_RATE");
        if (acceptRateEnv != null && !acceptRateEnv.isEmpty()) {
            try {
                acceptRate = Double.parseDouble(acceptRateEnv);
                releaseRate = (1.0 - acceptRate) * 0.75; // 75% of non-accepts are releases
            } catch (NumberFormatException e) {
                System.err.println("Invalid CHEF_ACCEPT_RATE, using default 0.80");
            }
        }

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", "chefs-share-group");
        props.setProperty("client.id", chefName.replaceAll("\\s+", "-"));
        props.setProperty("share.acknowledgement.mode", "explicit");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "com.example.qtest.OrderDeserializer");
        props.setProperty("max.poll.records", "1");
        props.setProperty("max.poll.interval.ms", "300000");

        // Add SASL/SSL config if API key/secret are provided (for Confluent Cloud)
        String apiKey = System.getenv("CONFLUENT_API_KEY");
        String apiSecret = System.getenv("CONFLUENT_API_SECRET");
        if (apiKey != null && apiSecret != null && !apiKey.isEmpty() && !apiSecret.isEmpty()) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));
        }

        Random random = new Random();

        System.out.println("Creating KafkaShareConsumer...");

        try (KafkaShareConsumer<String, Order> consumer = new KafkaShareConsumer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutdown signal received, stopping consumer...");
                running.set(false);
                reportToDashboard(chefName, null, "SHUTDOWN", null, 0);
            }));

            consumer.subscribe(Arrays.asList(TOPIC));
            System.out.printf("%n✅ Subscribed to topic '%s' (group='%s')%n",
                TOPIC, props.getProperty("group.id"));
            System.out.printf("⚙️  Accept rate: %.0f%%, Release rate: %.0f%%, Reject rate: %.0f%%%n",
                acceptRate * 100, releaseRate * 100, (1.0 - acceptRate - releaseRate) * 100);
            System.out.printf("⏱️  Processing delay: %d ms%n", processingDelayMs);
            System.out.println();
            
            // Report to dashboard immediately so chef appears before processing orders
            reportToDashboard(chefName, null, "STARTED", null, 0);
            long lastHeartbeat = System.currentTimeMillis();

            printChef(chefName);

            while (running.get()) {
                ConsumerRecords<String, Order> records;
                try {
                    records = consumer.poll(Duration.ofMillis(1000));
                } catch (RecordDeserializationException e) {
                    // Poison pill: the broker delivered a record this consumer's Deserializer
                    // couldn't parse. RecordDeserializationException carries the offending
                    // topic/partition/offset even though no value could be produced, so we can
                    // reject it directly by coordinate instead of crashing the whole consumer.
                    String badTopic = e.topicPartition().topic();
                    int badPartition = e.topicPartition().partition();
                    long badOffset = e.offset();
                    String cause = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                    System.out.printf("%n💀 Poison pill at %s-%d@%d: %s%n", badTopic, badPartition, badOffset, cause);

                    consumer.acknowledge(badTopic, badPartition, badOffset, AcknowledgeType.REJECT);
                    Map<TopicIdPartition, Optional<KafkaException>> commitResult = consumer.commitSync();
                    for (Optional<KafkaException> commitException : commitResult.values()) {
                        if (commitException.isPresent()) {
                            System.out.printf("⚠️ Commit warning after poison-pill reject: %s%n",
                                commitException.get().getMessage());
                        }
                    }

                    String syntheticOrderId = "POISON@" + badTopic + "-" + badPartition + "-" + badOffset;
                    reportToDashboard(chefName, syntheticOrderId, "REJECTED", null, 0);
                    continue;
                }

                if (records.isEmpty()) {
                    long now = System.currentTimeMillis();
                    if (now - lastHeartbeat > 3_000) {
                        reportToDashboard(chefName, null, "IDLE", null, 0);
                        lastHeartbeat = now;
                    }
                    continue;
                }
                lastHeartbeat = System.currentTimeMillis();

                for (ConsumerRecord<String, Order> r : records) {
                    if (!running.get()) break;

                    Order order = r.value();
                    if (order == null) {
                        System.out.println("⚠️ Received null order, skipping...");
                        continue;
                    }

                    order.setChefName(chefName);
                    System.out.printf("%n✨ Received: %s with %d items (offset=%d | delivery count: %d)%n",
                        order.getOrderId(), 
                        order.getOrderItems() != null ? order.getOrderItems().size() : 0,
                        r.offset(), 
                        r.deliveryCount().get());

                    try {
                        Thread.sleep(processingDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    double decision = random.nextDouble();
                    AcknowledgeType ackType;
                    String action;

                    if (decision < acceptRate) {
                        ackType = AcknowledgeType.ACCEPT;
                        order.setStatus("ACCEPTED");
                        action = "ACCEPTED";
                    } else if (decision < acceptRate + releaseRate) {
                        ackType = AcknowledgeType.RELEASE;
                        order.setStatus("RELEASED");
                        action = "RELEASED";
                    } else {
                        ackType = AcknowledgeType.REJECT;
                        order.setStatus("REJECTED");
                        action = "REJECTED";
                    }

                    String actionEmoji = action.equals("ACCEPTED") ? "✅" : 
                                       action.equals("RELEASED") ? "↩️" : "❌";
                    System.out.printf("%s %s: %s%n", actionEmoji, action.toLowerCase(), order.getOrderId());
                    consumer.acknowledge(r, ackType);
                    
                    Map<TopicIdPartition, Optional<KafkaException>> commitResult = consumer.commitSync();
                    boolean commitSuccess = true;
                    for (Optional<KafkaException> exception : commitResult.values()) {
                        if (exception.isPresent()) {
                            String msg = exception.get().getMessage();
                            if (!msg.contains("record state is invalid")) {
                                System.out.printf("⚠️ Commit warning: %s%n", msg);
                            }
                            commitSuccess = false;
                        }
                    }
                    
                    if (commitSuccess) {
                        int deliveryCount = r.deliveryCount().orElse((short) 1);
                        if (ackType == AcknowledgeType.ACCEPT) {
                            reportToDashboard(chefName, order.getOrderId(), action, order, deliveryCount);
                        } else {
                            reportToDashboard(chefName, order.getOrderId(), action, null, deliveryCount);
                        }
                    }
                    
                    System.out.println();
                    printChef(chefName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void reportToDashboard(String chefName, String orderId, String action, Order order, int deliveryCount) {
        String dashboardUrl = System.getenv("DASHBOARD_URL");
        if (dashboardUrl == null || dashboardUrl.isEmpty()) {
            dashboardUrl = "http://localhost:8080";
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            
            @SuppressWarnings("deprecation")
            URL url = new URL(dashboardUrl + "/api/chefs/" + chefName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> data = new HashMap<>();
            data.put("orderId", orderId);
            data.put("action", action);
            data.put("deliveryCount", deliveryCount);
            if (order != null && order.getOrderItems() != null) {
                data.put("orderItems", order.getOrderItems());
            }

            String json = mapper.writeValueAsString(data);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = json.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception e) { }
    }
}