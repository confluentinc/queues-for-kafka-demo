package com.example.qtest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standard Kafka consumer for inventory tracking/analytics.
 * This consumer reads all accepted orders and updates inventory counts.
 */
public class InventoryConsumer {

    private static final String TOPIC = "orders-queue";
    private static final String GROUP_ID = "inventory-analytics-group";
    
    // Initial inventory - starting stock for each menu item
    private static final Map<String, Integer> INITIAL_INVENTORY = new HashMap<>();
    static {
        INITIAL_INVENTORY.put("Pizza Margherita", 50);
        INITIAL_INVENTORY.put("Pizza Pepperoni", 50);
        INITIAL_INVENTORY.put("Pasta Carbonara", 40);
        INITIAL_INVENTORY.put("Caesar Salad", 30);
        INITIAL_INVENTORY.put("Burger", 40);
        INITIAL_INVENTORY.put("Fish & Chips", 35);
        INITIAL_INVENTORY.put("Steak", 30);
        INITIAL_INVENTORY.put("Chicken Wings", 45);
        INITIAL_INVENTORY.put("Soup", 40);
        INITIAL_INVENTORY.put("Dessert", 50);
    }

    private final Map<String, Integer> inventory = new ConcurrentHashMap<>(INITIAL_INVENTORY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<InventoryUpdateListener> listeners = new ArrayList<>();
    private final AtomicLong messagesSeenCount = new AtomicLong(0); // Count actual messages for queue depth

    public interface InventoryUpdateListener {
        void onInventoryUpdate(Map<String, Integer> inventory);
    }

    public void addListener(InventoryUpdateListener listener) {
        listeners.add(listener);
    }

    public Map<String, Integer> getCurrentInventory() {
        return new HashMap<>(inventory);
    }
    
    /**
     * Returns the number of messages seen since we started (or since last reset).
     * This is used for queue depth calculation.
     */
    public long getMessagesSeen() {
        return messagesSeenCount.get();
    }
    
    /**
     * Resets the message counter so that messagesSeen starts counting from zero.
     * Called when the dashboard resets to ensure queue depth calculations are accurate.
     */
    public void resetOffsetBaseline() {
        messagesSeenCount.set(0);
        System.out.println("📊 Message counter reset - queue depth will count from next message");
    }

    public void start() {
        // Get bootstrap servers from environment or use default
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        String clientId = "inventory-consumer-" + java.lang.ProcessHandle.current().pid();
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.example.qtest.OrderDeserializer");
        // Use "latest" so we only count messages that arrive after dashboard starts.
        // This aligns with the fact that chefs can only report to the dashboard while it's running.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

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

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║      📊 Inventory Analytics Consumer      ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  Topic: %-33s ║%n", TOPIC);
        System.out.printf("║  Group: %-33s ║%n", GROUP_ID);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
        System.out.println("✅ Inventory consumer started. Tracking new orders...");
        System.out.println();

        final Thread mainThread = Thread.currentThread();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutdown signal received...");
            running.set(false);
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            while (running.get()) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, Order> record : records) {
                    // Count messages for queue depth calculation
                    messagesSeenCount.incrementAndGet();
                    
                    Order order = record.value();
                    if (order == null) {
                        continue;
                    }

                    if (order.getOrderItems() != null) {
                        if ("ACCEPTED".equals(order.getStatus()) && order.getOrderItems() != null) {
                            boolean inventoryChanged = false;
                            for (OrderItem item : order.getOrderItems()) {
                                String itemName = item.getItemName();
                                int quantity = item.getQuantity();
                                
                                inventory.compute(itemName, (key, current) -> {
                                    if (current == null) {
                                        return Math.max(0, INITIAL_INVENTORY.getOrDefault(itemName, 0) - quantity);
                                    }
                                    return Math.max(0, current - quantity); // Don't go below 0
                                });
                                inventoryChanged = true;
                            }

                            if (inventoryChanged) {
                                System.out.printf("📦 Order %s (ACCEPTED): Inventory updated%n", order.getOrderId());
                                notifyListeners();
                            }
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown
        } finally {
            consumer.close();
            System.out.println("Inventory consumer closed.");
        }
    }

    private void notifyListeners() {
        Map<String, Integer> snapshot = getCurrentInventory();
        for (InventoryUpdateListener listener : listeners) {
            try {
                listener.onInventoryUpdate(snapshot);
            } catch (Exception e) {
                System.err.println("Error notifying listener: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        InventoryConsumer consumer = new InventoryConsumer();
        consumer.start();
    }
}
