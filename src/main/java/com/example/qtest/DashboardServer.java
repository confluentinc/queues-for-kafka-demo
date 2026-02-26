package com.example.qtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import spark.Spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple HTTP server for the dashboard UI.
 * Provides REST API endpoints for inventory and chef status.
 */
public class DashboardServer {

    private static final String STATE_FILE = "dashboard-state.json";
    private static final String ORDERS_TOPIC = "orders-queue";
    private static final String SHARE_GROUP_ID = "chefs-share-group";
    private static final String[] MENU_ITEMS = {
        "Pizza Margherita", "Pizza Pepperoni", "Pasta Carbonara",
        "Caesar Salad", "Burger", "Fish & Chips", "Steak",
        "Chicken Wings", "Soup", "Dessert"
    };
    private static final Random random = new Random();
    
    private static final long CHEF_STALE_THRESHOLD_MS = 5_000; // 5 seconds
    private static final int SEND_TIMEOUT_SECONDS = 5;
    private static final int MAX_CONSECUTIVE_FAILURES = 3;

    private final InventoryConsumer inventoryConsumer;
    private final ChefAutoScaler autoScaler;
    private final boolean kedaMode;
    private final String prometheusUrl;
    private final int kedaThreshold;
    private final int kedaMinReplicas;
    private final int kedaMaxReplicas;
    private final Map<String, ChefStatus> chefStatuses = new ConcurrentHashMap<>();
    private final Map<String, Integer> inventory = new ConcurrentHashMap<>();
    private final Set<String> processedOrders = ConcurrentHashMap.newKeySet();
    private final Set<String> rejectedOrders = ConcurrentHashMap.newKeySet();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final int port;
    private final ScheduledExecutorService stateScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService prometheusPoller;
    private ScheduledExecutorService lagPoller;
    private AdminClient adminClient;
    private volatile long cachedShareGroupLag = 0;
    private KafkaProducer<String, Order> producer;
    private final Properties producerProps;
    
    // Baseline for fallback queue depth calculation (used when AdminClient is unavailable)
    private volatile long completedBaseline = 0;

    // Track orders produced by the dashboard (used in generate response and fallback queue depth)
    private final AtomicLong ordersProduced = new AtomicLong(0);
    private final AtomicLong orderSequence = new AtomicLong(0);

    // Smoothed queue depth: track completions between Prometheus updates so the
    // dashboard shows a gradual decrease instead of jumping in coarse steps.
    private volatile long lastPrometheusBacklog = -1;
    private final AtomicLong completionsSinceSync = new AtomicLong(0);

    // Initial inventory
    {
        inventory.put("Pizza Margherita", 50);
        inventory.put("Pizza Pepperoni", 50);
        inventory.put("Pasta Carbonara", 40);
        inventory.put("Caesar Salad", 30);
        inventory.put("Burger", 40);
        inventory.put("Fish & Chips", 35);
        inventory.put("Steak", 30);
        inventory.put("Chicken Wings", 45);
        inventory.put("Soup", 40);
        inventory.put("Dessert", 50);
    }

    public static class ChefStatus {
        public String chefName;
        public String lastOrderId;
        public String lastAction;
        public long lastUpdateTime;
        public int totalOrdersProcessed;
        public int lastOrderDeliveryCount;
    }

    public DashboardServer(InventoryConsumer inventoryConsumer, ChefAutoScaler autoScaler, int port, Properties producerProps) {
        this(inventoryConsumer, autoScaler, port, producerProps, null, false, null, 5, 1, 4);
    }

    public DashboardServer(InventoryConsumer inventoryConsumer, ChefAutoScaler autoScaler, int port, Properties producerProps,
                           Properties adminProps,
                           boolean kedaMode, String prometheusUrl, int kedaThreshold, int kedaMinReplicas, int kedaMaxReplicas) {
        this.inventoryConsumer = inventoryConsumer;
        this.autoScaler = autoScaler;
        this.port = port;
        this.producerProps = producerProps;
        this.kedaMode = kedaMode;
        this.prometheusUrl = prometheusUrl;
        this.kedaThreshold = kedaThreshold;
        this.kedaMinReplicas = kedaMinReplicas;
        this.kedaMaxReplicas = kedaMaxReplicas;

        // Create AdminClient for querying actual share group lag (non-KEDA mode)
        if (!kedaMode && adminProps != null) {
            try {
                this.adminClient = AdminClient.create(adminProps);
                System.out.println("📊 AdminClient created for share group lag queries");
            } catch (Exception e) {
                System.err.println("⚠️ Could not create AdminClient: " + e.getMessage());
            }
        }

        loadState();

        inventoryConsumer.addListener(inv -> inventory.putAll(inv));
    }
    
    private void initProducer() {
        if (producer == null && producerProps != null) {
            try {
                producer = new KafkaProducer<>(producerProps);
                System.out.println("📤 Kafka producer initialized for order generation");
            } catch (Exception e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                System.err.println("❌ Failed to create Kafka producer: " + cause.getMessage());
                producer = null;
            }
        }
    }
    
    private List<OrderItem> generateRandomOrderItems() {
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
    
    private Map<String, Object> generateOrders(int count) {
        Map<String, Object> result = new HashMap<>();
        initProducer();
        if (producer == null) {
            System.err.println("❌ Producer not available - cannot generate orders");
            result.put("status", "error");
            result.put("error", "Producer not available");
            result.put("generated", 0);
            result.put("failed", count);
            result.put("requested", count);
            return result;
        }

        int successCount = 0;
        int failCount = 0;
        int consecutiveFailures = 0;
        String lastError = null;

        for (int i = 0; i < count; i++) {
            String orderId = String.format("ORD-%06d", orderSequence.incrementAndGet());
            List<OrderItem> orderItems = generateRandomOrderItems();
            Order order = new Order(orderId, orderItems);

            ProducerRecord<String, Order> record = new ProducerRecord<>(ORDERS_TOPIC, orderId, order);
            try {
                RecordMetadata metadata = producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.printf("✅ Generated: %s with %d items (partition=%d | offset=%d) [%d/%d]%n",
                        orderId, orderItems.size(), metadata.partition(), metadata.offset(), i + 1, count);
                successCount++;
                consecutiveFailures = 0;
                ordersProduced.incrementAndGet(); // Track for queue depth
            } catch (TimeoutException e) {
                failCount++;
                consecutiveFailures++;
                lastError = "Kafka send timed out";
                System.err.printf("❌ Timeout sending order %s [%d/%d] (consecutive failures: %d)%n",
                        orderId, i + 1, count, consecutiveFailures);
            } catch (Exception e) {
                failCount++;
                consecutiveFailures++;
                lastError = e.getMessage();
                System.err.printf("❌ Failed to send order %s: %s [%d/%d]%n",
                        orderId, e.getMessage(), i + 1, count);
            }

            if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                int remaining = count - i - 1;
                failCount += remaining;
                System.err.printf("🛑 Bailing out after %d consecutive failures (%d remaining orders skipped)%n",
                        MAX_CONSECUTIVE_FAILURES, remaining);
                break;
            }
        }

        result.put("requested", count);
        result.put("generated", successCount);
        result.put("failed", failCount);
        if (successCount == count) {
            result.put("status", "success");
        } else if (successCount == 0) {
            result.put("status", "error");
            if (lastError != null) result.put("error", lastError);
        } else {
            result.put("status", "partial");
            if (lastError != null) result.put("error", lastError);
        }
        return result;
    }
    
    private static class PersistedState {
        public Set<String> processedOrders = new HashSet<>();
        public Set<String> rejectedOrders = new HashSet<>();
        public Map<String, Integer> inventory = new HashMap<>();
    }
    
    private void loadState() {
        File stateFile = new File(STATE_FILE);
        if (stateFile.exists()) {
            try {
                PersistedState state = objectMapper.readValue(stateFile, PersistedState.class);
                if (state.processedOrders != null) {
                    processedOrders.addAll(state.processedOrders);
                }
                if (state.rejectedOrders != null) {
                    rejectedOrders.addAll(state.rejectedOrders);
                }
                // Set baseline so queue depth only tracks activity since this restart
                // This is needed because InventoryConsumer starts fresh (from "latest")
                // but we've loaded historical completed orders from disk
                completedBaseline = processedOrders.size() + rejectedOrders.size();
                
                System.out.printf("📂 Loaded state: %d processed, %d rejected orders (baseline=%d)%n", 
                    processedOrders.size(), rejectedOrders.size(), completedBaseline);
            } catch (IOException e) {
                System.out.println("📂 Could not load state file, starting fresh: " + e.getMessage());
            }
        } else {
            System.out.println("📂 No state file found, starting fresh");
        }
    }
    
    private void saveState() {
        try {
            PersistedState state = new PersistedState();
            state.processedOrders = new HashSet<>(processedOrders);
            state.rejectedOrders = new HashSet<>(rejectedOrders);
            state.inventory = new HashMap<>(inventory);
            objectMapper.writeValue(new File(STATE_FILE), state);
        } catch (IOException e) {
            System.err.println("⚠️ Could not save state file: " + e.getMessage());
        }
    }

    public void start() {
        Spark.port(port);
        Spark.staticFiles.location("/public");
        
        Spark.before((request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            response.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        });
        
        // Handle CORS preflight requests
        Spark.options("/*", (request, response) -> {
            return "OK";
        });

        Spark.get("/api/inventory", (req, res) -> {
            res.type("application/json");
            return objectMapper.writeValueAsString(inventory);
        });

        Spark.get("/api/chefs", (req, res) -> {
            res.type("application/json");
            if (autoScaler != null) {
                List<String> activeNames = autoScaler.getActiveChefNames();
                // Remove terminated chefs from our map
                chefStatuses.keySet().removeIf(name -> !activeNames.contains(name));
            } else {
                // No autoScaler (KEDA mode or standalone) — evict stale chefs
                long now = System.currentTimeMillis();
                chefStatuses.entrySet().removeIf(e ->
                        (now - e.getValue().lastUpdateTime) > CHEF_STALE_THRESHOLD_MS);
            }

            return objectMapper.writeValueAsString(chefStatuses.values());
        });

        Spark.post("/api/chefs/:chefName", (req, res) -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = objectMapper.readValue(req.body(), Map.class);
                String chefName = req.params(":chefName");
                String orderId = (String) data.get("orderId");
                String action = (String) data.get("action");

                // Handle SHUTDOWN: immediately remove the chef and return early
                if ("SHUTDOWN".equals(action)) {
                    chefStatuses.remove(chefName);
                    System.out.printf("👋 Chef %s shut down, removed from dashboard%n", chefName);
                    res.type("application/json");
                    return "{\"status\": \"removed\"}";
                }

                ChefStatus status = chefStatuses.computeIfAbsent(chefName, k -> {
                    ChefStatus s = new ChefStatus();
                    s.chefName = chefName;
                    s.totalOrdersProcessed = 0;
                    return s;
                });
                
                status.lastAction = action;
                status.lastUpdateTime = System.currentTimeMillis();
                
                // Only update order tracking for actual order actions, not startup notifications
                if (orderId != null) {
                    status.lastOrderId = orderId;
                    status.totalOrdersProcessed++;
                }
                
                // Extract delivery count from Kafka Share Group metrics (KIP-932)
                Object deliveryCountObj = data.get("deliveryCount");
                if (deliveryCountObj != null) {
                    status.lastOrderDeliveryCount = deliveryCountObj instanceof Integer ? (Integer) deliveryCountObj :
                                                   deliveryCountObj instanceof Number ? ((Number) deliveryCountObj).intValue() : 0;
                }
                
                if (orderId != null) {
                    if ("ACCEPTED".equals(action) && !processedOrders.contains(orderId)) {
                        @SuppressWarnings("unchecked")
                        java.util.List<Map<String, Object>> orderItems = 
                            (java.util.List<Map<String, Object>>) data.get("orderItems");
                        
                        if (orderItems != null) {
                            for (Map<String, Object> item : orderItems) {
                                String itemName = (String) item.get("itemName");
                                Object qtyObj = item.get("quantity");
                                int quantity = qtyObj instanceof Integer ? (Integer) qtyObj :
                                             qtyObj instanceof Number ? ((Number) qtyObj).intValue() : 1;

                                inventory.compute(itemName, (key, current) -> {
                                    if (current == null) {
                                        return Math.max(0, inventory.getOrDefault(itemName, 50) - quantity);
                                    }
                                    return Math.max(0, current - quantity);
                                });
                            }
                        }
                        processedOrders.add(orderId);
                        completionsSinceSync.incrementAndGet();
                        System.out.printf("📦 Order accepted: %s (delivery count: %d)%n", orderId, status.lastOrderDeliveryCount);
                    } else if ("REJECTED".equals(action) && !rejectedOrders.contains(orderId)) {
                        rejectedOrders.add(orderId);
                        completionsSinceSync.incrementAndGet();
                        System.out.printf("🗑️ Order rejected: %s%n", orderId);
                    }
                }
                
                res.type("application/json");
                return objectMapper.writeValueAsString(status);
            } catch (Exception e) {
                res.status(400);
                return "{\"error\": \"" + e.getMessage() + "\"}";
            }
        });

        Spark.post("/api/inventory/update", (req, res) -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Integer> updates = objectMapper.readValue(req.body(), Map.class);
                inventory.putAll(updates);
                res.type("application/json");
                return objectMapper.writeValueAsString(inventory);
            } catch (Exception e) {
                res.status(400);
                return "{\"error\": \"" + e.getMessage() + "\"}";
            }
        });
        
        Spark.post("/api/inventory/restock", (req, res) -> {
            res.type("application/json");
            int amount = 25; // Default restock amount
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> body = objectMapper.readValue(req.body(), Map.class);
                if (body != null && body.containsKey("amount")) {
                    amount = ((Number) body.get("amount")).intValue();
                }
            } catch (Exception ignored) {
                // Use default amount if body parsing fails
            }
            
            final int restockAmount = amount;
            inventory.replaceAll((item, currentQty) -> currentQty + restockAmount);
            
            System.out.printf("📦 Restocked inventory: +%d to each item%n", restockAmount);
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "success");
            result.put("amountAdded", restockAmount);
            result.put("inventory", inventory);
            return objectMapper.writeValueAsString(result);
        });

        Spark.get("/api/orders/accepted-count", (req, res) -> {
            res.type("application/json");
            Map<String, Object> result = new HashMap<>();
            long completedCount = processedOrders.size() + rejectedOrders.size();
            result.put("acceptedCount", completedCount);
            result.put("accepted", processedOrders.size());
            result.put("rejected", rejectedOrders.size());
            return objectMapper.writeValueAsString(result);
        });

        Spark.get("/api/autoscale", (req, res) -> {
            res.type("application/json");
            Map<String, Object> status = new HashMap<>();

            if (kedaMode) {
                // KEDA mode: get queue depth from lag exporter, count non-stale chefs,
                // and compute the target replica count using the same formula KEDA uses:
                //   desired = clamp(ceil(backlog / threshold), min, max)
                long queueDepth = fetchQueueDepthFromPrometheus();
                long now = System.currentTimeMillis();
                long activeChefs = chefStatuses.values().stream()
                        .filter(s -> (now - s.lastUpdateTime) <= CHEF_STALE_THRESHOLD_MS)
                        .count();

                int targetChefs;
                if (queueDepth == 0) {
                    targetChefs = kedaMinReplicas;
                } else {
                    targetChefs = Math.min(kedaMaxReplicas,
                            Math.max(kedaMinReplicas, (int) Math.ceil((double) queueDepth / kedaThreshold)));
                }

                status.put("enabled", true);
                status.put("kedaMode", true);
                status.put("queueDepth", queueDepth);
                status.put("activeChefs", activeChefs);
                status.put("targetChefs", targetChefs);
                status.put("maxChefs", kedaMaxReplicas);
                status.put("minChefs", kedaMinReplicas);
                status.put("threshold", kedaThreshold);
                status.put("rejectedOrders", rejectedOrders.size());
            } else {
                // Local mode
                long queueDepth = localQueueDepth();
                status.put("queueDepth", queueDepth);

                if (autoScaler != null) {
                    status.put("activeChefs", autoScaler.getActiveChefCount());
                    status.put("targetChefs", autoScaler.getTargetChefCount());
                    status.put("activeChefNames", autoScaler.getActiveChefNames());
                    status.put("rejectedOrders", rejectedOrders.size());
                    status.put("enabled", true);
                    status.put("manualMode", autoScaler.isManualMode());
                    status.put("maxChefs", autoScaler.getMaxChefs());
                    status.put("minChefs", autoScaler.getMinChefs());
                } else {
                    status.put("enabled", false);
                }
            }
            return objectMapper.writeValueAsString(status);
        });
        
        Spark.post("/api/scaling/add-chef", (req, res) -> {
            res.type("application/json");
            if (autoScaler == null) {
                res.status(400);
                return "{\"error\": \"Scaling not enabled\"}";
            }
            if (!autoScaler.isManualMode()) {
                res.status(400);
                return "{\"error\": \"Manual scaling not enabled. Set MANUAL_SCALE_MODE=true\"}";
            }
            if (autoScaler.getActiveChefCount() >= autoScaler.getMaxChefs()) {
                res.status(400);
                return "{\"error\": \"Maximum chef limit reached\"}";
            }
            autoScaler.spawnChef();
            Map<String, Object> result = new HashMap<>();
            result.put("status", "success");
            result.put("activeChefs", autoScaler.getActiveChefCount());
            return objectMapper.writeValueAsString(result);
        });
        
        Spark.post("/api/scaling/remove-chef", (req, res) -> {
            res.type("application/json");
            if (autoScaler == null) {
                res.status(400);
                return "{\"error\": \"Scaling not enabled\"}";
            }
            if (!autoScaler.isManualMode()) {
                res.status(400);
                return "{\"error\": \"Manual scaling not enabled. Set MANUAL_SCALE_MODE=true\"}";
            }
            if (autoScaler.getActiveChefCount() <= autoScaler.getMinChefs()) {
                res.status(400);
                return "{\"error\": \"Minimum chef limit reached\"}";
            }
            autoScaler.terminateChef();
            Map<String, Object> result = new HashMap<>();
            result.put("status", "success");
            result.put("activeChefs", autoScaler.getActiveChefCount());
            return objectMapper.writeValueAsString(result);
        });
        
        Spark.get("/api/autoscale/queue-depth", (req, res) -> {
            res.type("application/json");
            if (kedaMode) {
                long queueDepth = fetchQueueDepthFromPrometheus();
                return "{\"queueDepth\":" + queueDepth + "}";
            }
            long queueDepth = localQueueDepth();
            return "{\"queueDepth\":" + queueDepth + "}";
        });

        Spark.get("/api/health", (req, res) -> {
            return "{\"status\": \"ok\"}";
        });
        
        Spark.post("/api/reset", (req, res) -> {
            processedOrders.clear();
            rejectedOrders.clear();
            // Reset baseline so queue depth starts fresh
            completedBaseline = 0;
            // Reset orders produced counter
            ordersProduced.set(0);
            orderSequence.set(0);
            // Reset smoothed queue depth tracking
            lastPrometheusBacklog = -1;
            completionsSinceSync.set(0);
            // Reset inventory consumer's offset tracking
            inventoryConsumer.resetOffsetBaseline();
            // Reset inventory to initial values
            inventory.clear();
            inventory.put("Pizza Margherita", 50);
            inventory.put("Pizza Pepperoni", 50);
            inventory.put("Pasta Carbonara", 40);
            inventory.put("Caesar Salad", 30);
            inventory.put("Burger", 40);
            inventory.put("Fish & Chips", 35);
            inventory.put("Steak", 30);
            inventory.put("Chicken Wings", 45);
            inventory.put("Soup", 40);
            inventory.put("Dessert", 50);
            chefStatuses.clear();
            saveState();
            System.out.println("🔄 State reset via API (queue depth baseline reset)");
            return "{\"status\": \"reset\"}";
        });
        
        Spark.post("/api/orders/generate", (req, res) -> {
            res.type("application/json");
            int count = 40; // Default to 40 orders
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> body = objectMapper.readValue(req.body(), Map.class);
                if (body != null && body.containsKey("count")) {
                    count = ((Number) body.get("count")).intValue();
                }
            } catch (Exception ignored) {
                // Use default count if body parsing fails
            }

            System.out.printf("🚀 Generating %d orders via dashboard...%n", count);
            try {
                Map<String, Object> result = generateOrders(count);

                if ("error".equals(result.get("status"))) {
                    res.status(502);
                }
                return objectMapper.writeValueAsString(result);
            } catch (Exception e) {
                System.err.println("❌ Unexpected error generating orders: " + e.getMessage());
                res.status(500);
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("status", "error");
                errorResult.put("error", "Internal error: " + e.getMessage());
                errorResult.put("generated", 0);
                errorResult.put("failed", count);
                errorResult.put("requested", count);
                return objectMapper.writeValueAsString(errorResult);
            }
        });

        Spark.init();
        stateScheduler.scheduleAtFixedRate(this::saveState, 10, 10, TimeUnit.SECONDS);

        if (kedaMode && prometheusUrl != null && !prometheusUrl.isEmpty()) {
            prometheusPoller = Executors.newSingleThreadScheduledExecutor();
            prometheusPoller.scheduleAtFixedRate(this::pollPrometheus, 0, 1, TimeUnit.SECONDS);
        }

        // In non-KEDA mode, poll the share group lag directly via AdminClient
        if (!kedaMode && adminClient != null) {
            lagPoller = Executors.newSingleThreadScheduledExecutor();
            lagPoller.scheduleAtFixedRate(this::pollShareGroupLag, 0, 2, TimeUnit.SECONDS);
        }
        
        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║         🌐 Dashboard Server Started       ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  URL: http://localhost:%d                  ║%n", port);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * Queries Prometheus for the same backlog metric KEDA uses.
     * This ensures the dashboard shows exactly the value KEDA acts on.
     * Falls back to local ordersProduced tracking if Prometheus is unavailable.
     */
    /**
     * Called by the background scheduler every second to fetch the latest
     * backlog value from Prometheus and update the cached baseline.
     */
    private void pollPrometheus() {
        long raw = fetchRawBacklogFromPrometheus();
        if (raw != lastPrometheusBacklog) {
            lastPrometheusBacklog = raw;
            completionsSinceSync.set(0);
        }
    }

    /**
     * Returns a smoothed queue depth for display — no network call, instant return.
     * Between Prometheus updates, subtracts completions reported by consumers
     * for a gradual per-order decrease.
     */
    private long fetchQueueDepthFromPrometheus() {
        if (lastPrometheusBacklog < 0) {
            return localQueueDepth();
        }
        return Math.max(0, lastPrometheusBacklog - completionsSinceSync.get());
    }

    /**
     * Fetches the raw qfk_share_group_backlog value from the Prometheus query API.
     */
    private long fetchRawBacklogFromPrometheus() {
        if (prometheusUrl == null || prometheusUrl.isEmpty()) {
            return localQueueDepth();
        }

        try {
            String query = java.net.URLEncoder.encode(
                    "qfk_share_group_backlog{share_group=\"chefs-share-group\"}", "UTF-8");
            URL url = new URL(prometheusUrl + "/api/v1/query?query=" + query);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);

            if (conn.getResponseCode() == 200) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    // Response: {"status":"success","data":{"resultType":"vector","result":[{"metric":{...},"value":[ts,"42"]}]}}
                    @SuppressWarnings("unchecked")
                    Map<String, Object> resp = objectMapper.readValue(sb.toString(), Map.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> data = (Map<String, Object>) resp.get("data");
                    @SuppressWarnings("unchecked")
                    java.util.List<Map<String, Object>> results = (java.util.List<Map<String, Object>>) data.get("result");
                    if (results != null && !results.isEmpty()) {
                        @SuppressWarnings("unchecked")
                        java.util.List<Object> value = (java.util.List<Object>) results.get(0).get("value");
                        return (long) Double.parseDouble(value.get(1).toString());
                    }
                    return 0;
                }
            }
        } catch (Exception e) {
            System.err.println("⚠️ Could not fetch backlog from Prometheus: " + e.getMessage());
        }

        return localQueueDepth();
    }

    /**
     * Polls the actual share group lag from Kafka via AdminClient.
     * Called periodically by the lagPoller scheduler.
     */
    private void pollShareGroupLag() {
        try {
            ListShareGroupOffsetsSpec spec = new ListShareGroupOffsetsSpec();
            Map<TopicPartition, SharePartitionOffsetInfo> offsets =
                    adminClient.listShareGroupOffsets(Map.of(SHARE_GROUP_ID, spec))
                            .partitionsToOffsetInfo(SHARE_GROUP_ID).get();

            long totalLag = 0;
            for (Map.Entry<TopicPartition, SharePartitionOffsetInfo> entry : offsets.entrySet()) {
                if (!entry.getKey().topic().equals(ORDERS_TOPIC)) continue;
                long lag = entry.getValue().lag().orElse(0L);
                if (lag > 0) totalLag += lag;
            }
            cachedShareGroupLag = totalLag;
        } catch (Exception e) {
            // Keep last known value on transient failures
        }
    }

    private long localQueueDepth() {
        if (adminClient != null) {
            return cachedShareGroupLag;
        }
        // Fallback if AdminClient is unavailable
        long totalProduced = ordersProduced.get();
        long totalCompleted = processedOrders.size() + rejectedOrders.size();
        long completedSinceBaseline = totalCompleted - completedBaseline;
        return Math.max(0, totalProduced - completedSinceBaseline);
    }

    private static int intEnv(String name, int defaultValue) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) return defaultValue;
        try { return Integer.parseInt(val); } catch (NumberFormatException e) { return defaultValue; }
    }

    public void stop() {
        System.out.println("💾 Saving state before shutdown...");
        saveState();
        stateScheduler.shutdown();
        try {
            if (!stateScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                stateScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            stateScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (prometheusPoller != null) {
            prometheusPoller.shutdownNow();
        }

        if (lagPoller != null) {
            lagPoller.shutdownNow();
        }

        if (adminClient != null) {
            adminClient.close();
        }

        if (producer != null) {
            producer.close();
            System.out.println("📤 Kafka producer closed");
        }
        
        if (autoScaler != null) {
            autoScaler.stop();
        }
        Spark.stop();
    }

    public static void main(String[] args) {
        int port = 8080;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port, using default 8080");
            }
        }

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        Properties adminProps = new Properties();
        adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        
        String apiKey = System.getenv("CONFLUENT_API_KEY");
        String apiSecret = System.getenv("CONFLUENT_API_SECRET");
        if (apiKey != null && apiSecret != null && !apiKey.isEmpty() && !apiSecret.isEmpty()) {
            adminProps.setProperty("security.protocol", "SASL_SSL");
            adminProps.setProperty("sasl.mechanism", "PLAIN");
            adminProps.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));
        }

        // Set up producer properties for order generation
        String dashboardProducerClientId = "dashboard-order-generator-" + ProcessHandle.current().pid();
        
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", bootstrapServers);
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, dashboardProducerClientId);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "com.example.qtest.JsonSerializer");
        producerProps.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        producerProps.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        producerProps.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");
        
        if (apiKey != null && apiSecret != null && !apiKey.isEmpty() && !apiSecret.isEmpty()) {
            producerProps.setProperty("security.protocol", "SASL_SSL");
            producerProps.setProperty("sasl.mechanism", "PLAIN");
            producerProps.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));
        }

        boolean kedaMode = "true".equalsIgnoreCase(System.getenv("KEDA_MODE"));
        String prometheusUrl = System.getenv("PROMETHEUS_URL");

        int kedaThreshold = intEnv("KEDA_THRESHOLD", 5);
        int kedaMinReplicas = intEnv("KEDA_MIN_REPLICAS", 1);
        int kedaMaxReplicas = intEnv("KEDA_MAX_REPLICAS", 4);

        InventoryConsumer inventoryConsumer = new InventoryConsumer();
        ChefAutoScaler autoScaler = null;

        if (!kedaMode) {
            String autoScaleEnabled = System.getenv("AUTO_SCALE_ENABLED");
            boolean manualMode = "true".equalsIgnoreCase(System.getenv("MANUAL_SCALE_MODE"));
            if (autoScaleEnabled == null || "true".equalsIgnoreCase(autoScaleEnabled)) {
                autoScaler = new ChefAutoScaler(bootstrapServers, adminProps, manualMode);
                autoScaler.start();
            }
        }

        DashboardServer server = new DashboardServer(inventoryConsumer, autoScaler, port, producerProps,
                adminProps, kedaMode, prometheusUrl, kedaThreshold, kedaMinReplicas, kedaMaxReplicas);
        
        Thread inventoryThread = new Thread(() -> {
            try {
                inventoryConsumer.start();
            } catch (Exception e) {
                System.err.println("Inventory consumer error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        inventoryThread.setDaemon(true);
        inventoryThread.start();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down dashboard server...");
            server.stop();
        }));

        try {
            inventoryThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
