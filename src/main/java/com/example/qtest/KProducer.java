package com.example.qtest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Automated Kafka producer that sends orders with order items.
 * Orders are generated automatically at intervals.
 */
public class KProducer {

    private static final String TOPIC = "orders-queue";
    private static final String[] MENU_ITEMS = {
        "Pizza Margherita", "Pizza Pepperoni", "Pasta Carbonara",
        "Caesar Salad", "Burger", "Fish & Chips", "Steak",
        "Chicken Wings", "Soup", "Dessert"
    };

    private static void printHeader() {
        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║         🍽️  StreamBytes Restaurant        ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.println("║  Waiters send orders to the kitchen via   ║");
        System.out.println("║  Kafka Queues (KIP-932 / Share Groups).   ║");
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }

    public static void main(String[] args) {
        printHeader();

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        long orderIntervalMs = 5000;
        String intervalEnv = System.getenv("ORDER_INTERVAL_MS");
        if (intervalEnv != null && !intervalEnv.isEmpty()) {
            try {
                orderIntervalMs = Long.parseLong(intervalEnv);
            } catch (NumberFormatException e) {
                System.err.println("Invalid ORDER_INTERVAL_MS, using default 5000ms");
            }
        }

        String producerClientId = "order-producer-" + java.lang.ProcessHandle.current().pid();
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.example.qtest.JsonSerializer");
        props.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "30000");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

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

        System.out.printf("🔍 Connecting to Kafka at %s...%n", bootstrapServers);
        if (!verifyTopicExists(bootstrapServers, props)) {
            System.err.println();
            System.err.println("❌ ERROR: Topic '" + TOPIC + "' does not exist or broker is not reachable.");
            System.err.println();
            System.err.println("📋 To fix this issue:");
            System.err.println();
            System.err.println("   For Local Kafka (Docker):");
            System.err.println("   1. Make sure Kafka is running:");
            System.err.println("      docker run --name kafka_qfk --rm -p 9092:9092 apache/kafka:4.1.1");
            System.err.println();
            System.err.println("   2. Create the topic:");
            System.err.println("      docker exec -it kafka_qfk sh");
            System.err.println("      /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1");
            System.err.println("      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-queue --partitions 1");
            System.err.println("      exit");
            System.err.println();
            System.err.println("   For Confluent Cloud:");
            System.err.println("   1. Create the topic using the Confluent Cloud UI or Terraform");
            System.err.println("   2. Verify your BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, and CONFLUENT_API_SECRET are set correctly");
            System.err.println();
            System.exit(1);
        }
        System.out.println("✅ Topic '" + TOPIC + "' exists and broker is reachable.");
        System.out.println();

        AtomicBoolean running = new AtomicBoolean(true);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutdown signal received...");
                running.set(false);
                try {
                    producer.flush();
                } catch (Exception ignored) {}
                producer.close();
                System.out.println("Producer closed.");
            }));

            Random random = new Random();

            System.out.println();
            System.out.println("🚀 Generating initial batch of 40 orders to demonstrate autoscaling...");
            System.out.println();
            
            // Generate 40 messages at startup to demonstrate autoscaling
            int initialBatchSize = 40;
            for (int i = 0; i < initialBatchSize && running.get(); i++) {
                String orderId = String.format("ORD-%04d", random.nextInt(10000));
                List<OrderItem> orderItems = generateRandomOrderItems(random);
                Order order = new Order(orderId, orderItems);

                ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, orderId, order);
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("✅ Sent: %s with %d items (partition=%d | offset=%d) [%d/%d]%n",
                            orderId, orderItems.size(), metadata.partition(), metadata.offset(), i + 1, initialBatchSize);
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    if (e.getCause() instanceof TimeoutException) {
                        System.err.println("❌ Failed to send message: " + errorMsg);
                        System.err.println("   This usually means the topic doesn't exist or the broker is unreachable.");
                        System.err.println("   Please verify the topic '" + TOPIC + "' exists and Kafka is running.");
                    } else {
                        System.err.println("❌ Failed to send message: " + errorMsg);
                    }
                }
            }
            
            System.out.println();
            System.out.printf("✅ Initial batch of %d orders sent!%n", initialBatchSize);
            System.out.println();
            System.out.printf("🛎️ Starting automated order generation (interval: %d ms)...%n", orderIntervalMs);
            System.out.println("Press [Ctrl+C] to stop...");
            System.out.println();

            while (running.get()) {
                String orderId = String.format("ORD-%04d", random.nextInt(10000));
                List<OrderItem> orderItems = generateRandomOrderItems(random);
                Order order = new Order(orderId, orderItems);

                ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, orderId, order);
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("✅ Sent: %s with %d items (partition=%d | offset=%d)%n",
                            orderId, orderItems.size(), metadata.partition(), metadata.offset());
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    if (e.getCause() instanceof TimeoutException) {
                        System.err.println("❌ Failed to send message: " + errorMsg);
                        System.err.println("   This usually means the topic doesn't exist or the broker is unreachable.");
                        System.err.println("   Please verify the topic '" + TOPIC + "' exists and Kafka is running.");
                    } else {
                        System.err.println("❌ Failed to send message: " + errorMsg);
                    }
                }

                try {
                    Thread.sleep(orderIntervalMs);
                } catch (InterruptedException e) {
                    running.set(false);
                    break;
                }
            }

            System.out.println("Exiting main loop...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean verifyTopicExists(String bootstrapServers, Properties producerProps) {
        Properties adminProps = new Properties();
        adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "order-producer-admin-" + java.lang.ProcessHandle.current().pid());
        adminProps.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        
        String securityProtocol = producerProps.getProperty("security.protocol");
        if (securityProtocol != null) {
            adminProps.setProperty("security.protocol", securityProtocol);
            adminProps.setProperty("sasl.mechanism", producerProps.getProperty("sasl.mechanism"));
            adminProps.setProperty("sasl.jaas.config", producerProps.getProperty("sasl.jaas.config"));
        }
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get(5, TimeUnit.SECONDS);
            return topics.contains(TOPIC);
        } catch (Exception e) {
            return false;
        }
    }

    private static List<OrderItem> generateRandomOrderItems(Random random) {
        List<OrderItem> items = new ArrayList<>();
        int numItems = random.nextInt(3) + 1; // 1-3 items per order
        
        Set<String> selectedItems = new HashSet<>();
        for (int i = 0; i < numItems; i++) {
            String itemName;
            do {
                itemName = MENU_ITEMS[random.nextInt(MENU_ITEMS.length)];
            } while (selectedItems.contains(itemName));
            selectedItems.add(itemName);
            
            int quantity = random.nextInt(2) + 1; // 1-2 quantity
            items.add(new OrderItem(itemName, quantity));
        }
        
        return items;
    }
}