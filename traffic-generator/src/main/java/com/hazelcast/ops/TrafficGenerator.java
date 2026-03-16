package com.hazelcast.ops;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Generates continuous traffic against CP data structures to produce
 * meaningful metrics in the Grafana dashboard.
 *
 * CP groups used: group1 … group7
 *   - CPMaps    : map0@group1 … map9@group1  (all maps in group1)
 *   - FencedLock: lock0@group2 … lock2@group2
 *   - ISemaphore: sem0@group3  … sem2@group3
 *   - IAtomicLong: counter0@group4 … counter4@group4
 *
 * Configuration via environment variables:
 *   HZ_CLUSTER_NAME  – cluster name         (default: dev)
 *   HZ_MEMBERS       – comma-separated hosts (default: hz1:5701,…,hz5:5701)
 *   KEY_SPACE        – keys per map          (default: 500)
 *   VALUE_SIZE       – payload bytes         (default: 512)
 *   INTERVAL_MS      – cycle ms              (default: 1000)
 *   WRITE_RATIO      – 0-100, % map writes   (default: 70)
 *   READ_RATIO       – 0-100, % map reads    (default: 20)
 */
public class TrafficGenerator {

    private static final String[] MAP_NAMES = {
        "map0@group1","map1@group1","map2@group1","map3@group1","map4@group1",
        "map5@group5","map6@group5","map7@group5","map8@group5","map9@group5"
    };

    private static final String[] LOCK_NAMES = {
        "lock0@group2", "lock1@group2", "lock2@group2"
    };

    private static final String[] SEMAPHORE_NAMES = {
        "sem0@group3", "sem1@group3", "sem2@group7"
    };

    private static final String[] COUNTER_NAMES = {
        "counter0@group4", "counter1@group4", "counter2@group4",
        "counter3@group6", "counter4@group6"
    };

    private static final int SEMAPHORE_PERMITS = 5;

    // ── counters ──────────────────────────────────────────────────────────────
    private static final LongAdder writes    = new LongAdder();
    private static final LongAdder reads     = new LongAdder();
    private static final LongAdder removes   = new LongAdder();
    private static final LongAdder locks     = new LongAdder();
    private static final LongAdder acquires  = new LongAdder();
    private static final LongAdder increments = new LongAdder();
    private static final LongAdder errors    = new LongAdder();

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

        // ── connect ───────────────────────────────────────────────────────────
        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(clusterName);
        Arrays.stream(members.split(","))
              .map(String::trim)
              .forEach(m -> cfg.getNetworkConfig().addAddress(m));
        cfg.getConnectionStrategyConfig()
           .getConnectionRetryConfig()
           .setClusterConnectTimeoutMillis(-1);

        System.out.printf("Connecting to cluster '%s' @ [%s]%n", clusterName, members);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        System.out.println("Connected.");

        CPSubsystem cp = client.getCPSubsystem();

        // ── acquire CPMap handles (group1) ────────────────────────────────────
        List<CPMap<String, byte[]>> maps = new ArrayList<>(MAP_NAMES.length);
        for (String name : MAP_NAMES) {
            maps.add(cp.getMap(name));
            System.out.println("  acquired CPMap: " + name);
        }

        // ── acquire FencedLock handles (group2) ───────────────────────────────
        List<FencedLock> lockList = new ArrayList<>(LOCK_NAMES.length);
        for (String name : LOCK_NAMES) {
            lockList.add(cp.getLock(name));
            System.out.println("  acquired FencedLock: " + name);
        }

        // ── acquire ISemaphore handles (group3) ───────────────────────────────
        List<ISemaphore> semList = new ArrayList<>(SEMAPHORE_NAMES.length);
        for (String name : SEMAPHORE_NAMES) {
            ISemaphore sem = cp.getSemaphore(name);
            sem.init(SEMAPHORE_PERMITS);
            semList.add(sem);
            System.out.println("  acquired ISemaphore: " + name + " (permits=" + SEMAPHORE_PERMITS + ")");
        }

        // ── acquire IAtomicLong handles (group4) ──────────────────────────────
        List<IAtomicLong> counterList = new ArrayList<>(COUNTER_NAMES.length);
        for (String name : COUNTER_NAMES) {
            counterList.add(cp.getAtomicLong(name));
            System.out.println("  acquired IAtomicLong: " + name);
        }

        // ── periodic stats reporter ───────────────────────────────────────────
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats-reporter");
            t.setDaemon(true);
            return t;
        });
        reporter.scheduleAtFixedRate(() -> {
            long w  = writes.sumThenReset();
            long r  = reads.sumThenReset();
            long d  = removes.sumThenReset();
            long l  = locks.sumThenReset();
            long a  = acquires.sumThenReset();
            long c  = increments.sumThenReset();
            long e  = errors.sumThenReset();
            System.out.printf(
                "[stats] writes=%d  reads=%d  removes=%d  locks=%d  semAcquires=%d  increments=%d  errors=%d%n",
                w, r, d, l, a, c, e);
        }, 5, 5, TimeUnit.SECONDS);

        // ── main loop ─────────────────────────────────────────────────────────
        Random rng     = new Random();
        byte[] payload = new byte[valueSize];
        int    keySeq  = 0;

        System.out.printf(
            "Starting: %d maps · %d locks · %d semaphores · %d counters · "
            + "%d keys · %dB values · %dms cycle%n",
            maps.size(), lockList.size(), semList.size(), counterList.size(),
            keySpace, valueSize, intervalMs);

        while (true) {
            long cycleStart = System.currentTimeMillis();

            // CPMap operations
            for (CPMap<String, byte[]> map : maps) {
                String key  = "key-" + (keySeq % keySpace);
                int    roll = rng.nextInt(100);
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
                    System.err.printf("map error %s key=%s: %s%n", map, key, ex.getMessage());
                }
            }

            // FencedLock: lock → do work → unlock
            for (FencedLock lock : lockList) {
                try {
                    lock.lock();
                    locks.increment();
                    Thread.sleep(2); // hold briefly to simulate real work
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("lock error %s: %s%n", lock, ex.getMessage());
                } finally {
                    try { lock.unlock(); } catch (Exception ignored) {}
                }
            }

            // ISemaphore: acquire → do work → release
            for (ISemaphore sem : semList) {
                try {
                    if (sem.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                        acquires.increment();
                        Thread.sleep(2);
                        sem.release();
                    }
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("semaphore error %s: %s%n", sem, ex.getMessage());
                }
            }

            // IAtomicLong: increment
            for (IAtomicLong counter : counterList) {
                try {
                    counter.incrementAndGet();
                    increments.increment();
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("counter error %s: %s%n", counter, ex.getMessage());
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
