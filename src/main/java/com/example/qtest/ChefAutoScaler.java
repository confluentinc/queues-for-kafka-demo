package com.example.qtest;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple auto-scaler for chef consumers based on queue depth.
 * 
 * Demonstrates KIP-932 Share Groups auto-scaling:
 * - Monitors the orders-queue topic via dashboard API
 * - Scales up when queue depth exceeds threshold
 * - Scales down when queue is empty or low
 * - Uses fast HTTP calls to dashboard (no blocking Kafka calls)
 */
public class ChefAutoScaler {
    private static final String CHEF_NAME_PREFIX = "Chef-Auto";
    
    // Configuration
    private final int minChefs;
    private final int maxChefs;
    private final int scaleUpThreshold;
    private final int scaleDownThreshold;
    private final long checkIntervalMs;
    private final String dashboardUrl;
    
    // State
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService processMonitor = Executors.newCachedThreadPool(); // Separate pool for process monitoring
    private final Map<String, Process> activeChefs = new ConcurrentHashMap<>();
    private final AtomicInteger chefCounter = new AtomicInteger(1);
    private volatile boolean running = false;
    private volatile long lastQueueDepth = 0;
    private volatile int targetChefCount = 1;
    private final boolean manualMode;
    
    public ChefAutoScaler(String bootstrapServers, Properties adminProps) {
        this(bootstrapServers, adminProps, false);
    }
    
    public ChefAutoScaler(String bootstrapServers, Properties adminProps, boolean manualMode) {
        this.manualMode = manualMode;
        this.dashboardUrl = System.getenv().getOrDefault("DASHBOARD_URL", "http://localhost:8080");
        // Clamped to at least 1: a lone backlog record (e.g. a single poison pill) can never
        // cross scaleUpThreshold on its own, so minChefs is the only thing guaranteeing a
        // consumer is ever listening. AUTO_SCALE_MIN_CHEFS=0 would otherwise strand it forever.
        this.minChefs = Math.max(1, Integer.parseInt(System.getenv().getOrDefault("AUTO_SCALE_MIN_CHEFS", "1")));
        this.maxChefs = Integer.parseInt(System.getenv().getOrDefault("AUTO_SCALE_MAX_CHEFS", "4"));
        this.scaleUpThreshold = Integer.parseInt(System.getenv().getOrDefault("AUTO_SCALE_UP_THRESHOLD", "5"));
        this.scaleDownThreshold = Integer.parseInt(System.getenv().getOrDefault("AUTO_SCALE_DOWN_THRESHOLD", "2"));
        this.checkIntervalMs = Long.parseLong(System.getenv().getOrDefault("AUTO_SCALE_CHECK_INTERVAL_MS", "3000"));
    }
    
    public void start() {
        if (running) return;
        running = true;
        
        printBanner();
        
        targetChefCount = minChefs;
        for (int i = 0; i < minChefs; i++) {
            spawnChef();
        }
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndScale();
            } catch (Throwable t) {
                System.err.println("⚠️ Autoscaler error (will retry): " + t.getMessage());
            }
        }, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cleanupDeadProcesses();
            } catch (Throwable t) { }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }
    
    public void stop() {
        if (!running) return;
        running = false;
        
        System.out.println("Stopping auto-scaler...");
        activeChefs.values().forEach(p -> { if (p != null && p.isAlive()) p.destroy(); });
        activeChefs.clear();
        
        scheduler.shutdown();
        processMonitor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            processMonitor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            processMonitor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Runs a scale check immediately instead of waiting for the next checkIntervalMs tick.
     * Used right after producing a single record (e.g. a poison pill) that needs a listening
     * consumer right away, rather than whenever the queue depth happens to be re-polled.
     */
    public void triggerImmediateScaleCheck() {
        if (!running) return;
        try {
            checkAndScale();
        } catch (Throwable t) {
            System.err.println("⚠️ Immediate scale check failed: " + t.getMessage());
        }
    }

    private void checkAndScale() {
        try {
            long queueDepth = getQueueDepthFromDashboard();
            lastQueueDepth = queueDepth;
            
            // In manual mode, only update queue depth metrics - don't auto-scale
            if (manualMode) {
                System.out.println();
                System.out.printf("═══ MANUAL MODE: Queue=%d | Chefs=%d ═══%n", 
                    queueDepth, activeChefs.size());
                System.out.flush();
                return;
            }
            
            int currentChefs = activeChefs.size();
            int newTarget = calculateTargetChefs(queueDepth);
            
            System.out.println();
            System.out.printf("═══ AUTOSCALER: Queue=%d | Chefs=%d | Target=%d ═══%n", 
                queueDepth, currentChefs, newTarget);
            
            if (newTarget > targetChefCount) {
                System.out.printf("⬆️  Scaling UP to %d chefs%n", newTarget);
            } else if (newTarget < targetChefCount) {
                System.out.printf("⬇️  Scaling DOWN to %d chefs%n", newTarget);
            }
            System.out.flush();
            
            targetChefCount = newTarget;
            
            while (activeChefs.size() < targetChefCount && activeChefs.size() < maxChefs) {
                spawnChef();
            }
            
            while (activeChefs.size() > targetChefCount && activeChefs.size() > minChefs) {
                terminateChef();
            }
            
        } catch (Exception e) {
            System.err.println("Error in auto-scaling: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private long getQueueDepthFromDashboard() {
        java.net.HttpURLConnection conn = null;
        try {
            @SuppressWarnings("deprecation")
            java.net.URL url = new java.net.URL(dashboardUrl + "/api/autoscale/queue-depth");
            conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(conn.getInputStream()))) {
                    String line = reader.readLine();
                    if (line != null && line.contains("queueDepth")) {
                        // Parse {"queueDepth": N}
                        String numStr = line.replaceAll("[^0-9]", "");
                        if (!numStr.isEmpty()) {
                            return Long.parseLong(numStr);
                        }
                    }
                }
            }
        } catch (Exception e) { }
        finally {
            if (conn != null) conn.disconnect();
        }
        return lastQueueDepth;
    }
    
    
    private int calculateTargetChefs(long queueDepth) {
        if (queueDepth == 0) {
            return minChefs;
        } else if (queueDepth >= scaleUpThreshold) {
            int needed = (int) Math.ceil((double) queueDepth / scaleUpThreshold);
            return Math.min(maxChefs, Math.max(minChefs, needed));
        } else if (queueDepth <= scaleDownThreshold) {
            return minChefs;
        } else {
            return Math.max(minChefs, targetChefCount);
        }
    }
    
    
    public void spawnChef() {
        String chefName = CHEF_NAME_PREFIX + "-" + chefCounter.getAndIncrement();
        
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "mvn", "-B", "-q", "exec:java",
                "-Dexec.mainClass=com.example.qtest.QConsumer",
                "-Dexec.args=" + chefName
            );
            pb.inheritIO();
            
            Map<String, String> env = pb.environment();
            if (System.getenv("BOOTSTRAP_SERVERS") != null) env.put("BOOTSTRAP_SERVERS", System.getenv("BOOTSTRAP_SERVERS"));
            if (System.getenv("CONFLUENT_API_KEY") != null) env.put("CONFLUENT_API_KEY", System.getenv("CONFLUENT_API_KEY"));
            if (System.getenv("CONFLUENT_API_SECRET") != null) env.put("CONFLUENT_API_SECRET", System.getenv("CONFLUENT_API_SECRET"));
            if (System.getenv("DASHBOARD_URL") != null) env.put("DASHBOARD_URL", System.getenv("DASHBOARD_URL"));
            env.put("CHEF_PROCESSING_DELAY_MS", System.getenv().getOrDefault("CHEF_PROCESSING_DELAY_MS", "2000"));
            String acceptRate = System.getenv("CHEF_ACCEPT_RATE");
            if (acceptRate != null) env.put("CHEF_ACCEPT_RATE", acceptRate);
            
            Process process = pb.start();
            activeChefs.put(chefName, process);
            
            System.out.printf("✅ Spawned: %s (PID: %d)%n", chefName, process.pid());
            
            processMonitor.submit(() -> {
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    activeChefs.remove(chefName);
                }
            });
            
        } catch (IOException e) {
            System.err.printf("❌ Failed to spawn %s: %s%n", chefName, e.getMessage());
        }
    }
    
    public void terminateChef() {
        if (activeChefs.isEmpty()) return;
        
        String chefName = activeChefs.keySet().iterator().next();
        Process process = activeChefs.remove(chefName);
        
        if (process != null && process.isAlive()) {
            System.out.printf("🛑 Terminated: %s%n", chefName);
            process.destroy();
            scheduler.schedule(() -> { if (process.isAlive()) process.destroyForcibly(); }, 5, TimeUnit.SECONDS);
        }
    }
    
    private void cleanupDeadProcesses() {
        activeChefs.entrySet().removeIf(e -> {
            boolean dead = e.getValue() == null || !e.getValue().isAlive();
            if (dead) {
                System.out.printf("💀 Dead process removed: %s%n", e.getKey());
            }
            return dead;
        });
    }
    
    private void printBanner() {
        System.out.println("╔═══════════════════════════════════════════╗");
        if (manualMode) {
            System.out.println("║      👆 Chef Scaler (MANUAL MODE)         ║");
        } else {
            System.out.println("║      🤖 Chef Auto-Scaler Starting         ║");
        }
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  Mode: %-35s ║%n", manualMode ? "Manual" : "Auto");
        System.out.printf("║  Min Chefs: %-30d ║%n", minChefs);
        System.out.printf("║  Max Chefs: %-30d ║%n", maxChefs);
        if (!manualMode) {
            System.out.printf("║  Scale Up Threshold: %-20d ║%n", scaleUpThreshold);
            System.out.printf("║  Scale Down Threshold: %-18d ║%n", scaleDownThreshold);
        }
        System.out.printf("║  Check Interval: %-23d ms ║%n", checkIntervalMs);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }
    
    public long getLastQueueDepth() { return lastQueueDepth; }
    public long getCurrentQueueDepth() { return lastQueueDepth; }
    public int getActiveChefCount() { return activeChefs.size(); }
    public int getTargetChefCount() { return targetChefCount; }
    public List<String> getActiveChefNames() { return new ArrayList<>(activeChefs.keySet()); }
    public boolean isManualMode() { return manualMode; }
    public int getMaxChefs() { return maxChefs; }
    public int getMinChefs() { return minChefs; }
}
