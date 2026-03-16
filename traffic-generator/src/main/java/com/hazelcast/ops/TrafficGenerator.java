package com.hazelcast.ops;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Generates continuous read/write/remove traffic against CPMap instances
 * (map0 … map9) to produce meaningful metrics in the Grafana dashboard.
 *
 * Configuration via environment variables:
 *   HZ_CLUSTER_NAME  – cluster name          (default: dev)
 *   HZ_MEMBERS       – comma-separated hosts  (default: localhost:5701,…,localhost:5705)
 *   HZ_DOCKER_NAMES  – internal Docker hostnames to translate FROM (default: hz1,hz2,hz3,hz4,hz5)
 *                      Each hzN:5701 is translated → localhost:5701+N-1 automatically.
 *   KEY_SPACE        – keys per map           (default: 500)
 *   VALUE_SIZE       – payload bytes          (default: 512)
 *   INTERVAL_MS      – write cycle ms         (default: 1000)
 *   WRITE_RATIO      – 0-100, % writes        (default: 70)
 *   READ_RATIO       – 0-100, % reads         (default: 20)
 *   // remainder is removes
 */
public class TrafficGenerator {

    private static final String[] MAP_NAMES = {
        "map0","map1","map2","map3","map4",
        "map5","map6","map7","map8","map9"
    };

    // ── counters ─────────────────────────────────────────────────────────────
    private static final LongAdder writes  = new LongAdder();
    private static final LongAdder reads   = new LongAdder();
    private static final LongAdder removes = new LongAdder();
    private static final LongAdder errors  = new LongAdder();

    public static void main(String[] args) throws InterruptedException {

        // ── config ────────────────────────────────────────────────────────────
        String clusterName = env("HZ_CLUSTER_NAME", "dev");
        String members     = env("HZ_MEMBERS",
                                 "hz1:5701,hz2:5701,hz3:5701,hz4:5701,hz5:5701");
        int    keySpace    = intEnv("KEY_SPACE",   500);
        int    valueSize   = intEnv("VALUE_SIZE",  512);
        long   intervalMs  = intEnv("INTERVAL_MS", 1000);
        int    writePct    = intEnv("WRITE_RATIO",  70);
        int    readPct     = intEnv("READ_RATIO",   20);
        // removePct = 100 - writePct - readPct (clamped to ≥ 0)

        // ── connect ───────────────────────────────────────────────────────────
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(clusterName);
        Arrays.stream(members.split(","))
              .map(String::trim)
              .forEach(m -> cfg.getNetworkConfig().addAddress(m));
        cfg.getConnectionStrategyConfig()
           .getConnectionRetryConfig()
           .setClusterConnectTimeoutMillis(-1); // retry indefinitely

        System.out.printf("Connecting to cluster '%s' @ [%s]%n", clusterName, members);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        System.out.println("Connected.");

        // ── acquire CPMap handles ─────────────────────────────────────────────
        CPSubsystem cp = client.getCPSubsystem();
        List<CPMap<String, byte[]>> maps = new ArrayList<>(MAP_NAMES.length);
        for (String name : MAP_NAMES) {
            maps.add(cp.getMap(name));
            System.out.println("  acquired CPMap: " + name);
        }

        // ── periodic stats reporter ───────────────────────────────────────────
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats-reporter");
            t.setDaemon(true);
            return t;
        });
        reporter.scheduleAtFixedRate(() -> {
            long w = writes.sumThenReset();
            long r = reads.sumThenReset();
            long d = removes.sumThenReset();
            long e = errors.sumThenReset();
            System.out.printf("[stats] writes/s=%d  reads/s=%d  removes/s=%d  errors/s=%d%n",
                              w, r, d, e);
        }, 5, 5, TimeUnit.SECONDS);

        // ── main loop ─────────────────────────────────────────────────────────
        Random rng     = new Random();
        byte[] payload = new byte[valueSize];
        int    keySeq  = 0;

        System.out.printf(
            "Starting: %d maps · %d keys · %dB values · %dms cycle · "
            + "write=%d%% read=%d%% remove=%d%%%n",
            MAP_NAMES.length, keySpace, valueSize, intervalMs,
            writePct, readPct, Math.max(0, 100 - writePct - readPct));

        while (true) {
            long cycleStart = System.currentTimeMillis();

            for (CPMap<String, byte[]> map : maps) {
                String key = "key-" + (keySeq % keySpace);
                int roll = rng.nextInt(100);
                try {
                    if (roll < writePct) {
                        rng.nextBytes(payload);
                        map.set(key, payload.clone());
                        writes.increment();
                    } else if (roll < writePct + readPct) {
                        map.get(key);
                        reads.increment();
                    } else {
                        map.remove(key);
                        removes.increment();
                    }
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("error on %s key=%s: %s%n",
                                      map, key, ex.getMessage());
                }
            }
            keySeq++;

            long elapsed = System.currentTimeMillis() - cycleStart;
            long sleep   = intervalMs - elapsed;
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }

    private static int intEnv(String key, int def) {
        try { return Integer.parseInt(env(key, String.valueOf(def))); }
        catch (NumberFormatException e) { return def; }
    }
}
