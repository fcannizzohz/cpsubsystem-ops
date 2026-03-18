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
 *   - CPMaps     : map0–map4@group1,  map5–map9@group5
 *   - FencedLock : lock0–lock2@group2
 *   - ISemaphore : sem0–sem1@group3,  sem2@group7
 *   - IAtomicLong: counter0–counter2@group4,  counter3–counter4@group6
 *
 * Configuration via environment variables:
 *   HZ_CLUSTER_NAME  – cluster name         (default: dev)
 *   HZ_MEMBERS       – comma-separated hosts (default: hz1:5701,…,hz5:5701)
 *   KEY_SPACE        – keys per map          (default: 500)
 *   VALUE_SIZE       – payload bytes         (default: 512)
 *   INTERVAL_MS      – base cycle ms         (default: 1000)
 *   WRITE_RATIO      – 0-100, % map writes   (default: 70)
 *   READ_RATIO       – 0-100, % map reads    (default: 20)
 *
 * Non-uniform load model
 * ──────────────────────
 * Traffic is intentionally non-uniform to produce interesting dashboard signals:
 *
 *   Burst mode (two-state Markov):
 *     NORMAL → BURST  with probability 0.05 per cycle
 *     BURST  → NORMAL with probability 0.25 per cycle
 *     In BURST, each map receives 3–8× the normal operations, creating
 *     sustained commit-rate spikes and visible commit-lag increases.
 *
 *   Hot-spot key distribution:
 *     70% of map accesses target the first 20 keys (Zipf-like skew).
 *     This concentrates write pressure on a small partition of each map.
 *
 *   Log-normal lock hold time:
 *     Median ≈ 2 ms; a 5% tail of long holds (50–250 ms) simulates slow
 *     critical sections and drives lock-wait and semaphore-utilisation metrics.
 *
 *   Variable semaphore drain:
 *     10% of acquire attempts drain 2–4 permits instead of 1, producing
 *     utilisation spikes visible in the Permit Utilisation Ratio panel.
 *
 *   Bulk-remove waves:
 *     3% chance per cycle (outside burst mode) of a remove wave that
 *     deletes ≈20% of the key space, creating sudden drops in CPMap size.
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

    // Hot-spot distribution: HOT_RATIO of accesses target the first HOT_KEYS keys.
    private static final int    HOT_KEYS  = 20;
    private static final double HOT_RATIO = 0.70;

    // Burst-mode Markov transition probabilities.
    private static final double BURST_ENTRY_PROB  = 0.05;
    private static final double BURST_EXIT_PROB   = 0.25;
    private static final int    BURST_OPS_MIN     = 3;
    private static final int    BURST_OPS_MAX     = 8;

    // Bulk-remove wave: probability per cycle (only outside burst mode).
    private static final double REMOVE_WAVE_PROB     = 0.03;
    private static final double REMOVE_WAVE_KEY_FRAC = 0.20; // fraction of keys to remove

    // ── counters ──────────────────────────────────────────────────────────────
    private static final LongAdder writes     = new LongAdder();
    private static final LongAdder reads      = new LongAdder();
    private static final LongAdder removes    = new LongAdder();
    private static final LongAdder locks      = new LongAdder();
    private static final LongAdder acquires   = new LongAdder();
    private static final LongAdder increments = new LongAdder();
    private static final LongAdder errors     = new LongAdder();

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

        // ── acquire CPMap handles ─────────────────────────────────────────────
        List<CPMap<String, byte[]>> maps = new ArrayList<>(MAP_NAMES.length);
        for (String name : MAP_NAMES) {
            maps.add(cp.getMap(name));
            System.out.println("  acquired CPMap: " + name);
        }

        // ── acquire FencedLock handles ────────────────────────────────────────
        List<FencedLock> lockList = new ArrayList<>(LOCK_NAMES.length);
        for (String name : LOCK_NAMES) {
            lockList.add(cp.getLock(name));
            System.out.println("  acquired FencedLock: " + name);
        }

        // ── acquire ISemaphore handles ────────────────────────────────────────
        List<ISemaphore> semList = new ArrayList<>(SEMAPHORE_NAMES.length);
        for (String name : SEMAPHORE_NAMES) {
            ISemaphore sem = cp.getSemaphore(name);
            sem.init(SEMAPHORE_PERMITS);
            semList.add(sem);
            System.out.println("  acquired ISemaphore: " + name + " (permits=" + SEMAPHORE_PERMITS + ")");
        }

        // ── acquire IAtomicLong handles ───────────────────────────────────────
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
            long w = writes.sumThenReset();
            long r = reads.sumThenReset();
            long d = removes.sumThenReset();
            long l = locks.sumThenReset();
            long a = acquires.sumThenReset();
            long c = increments.sumThenReset();
            long e = errors.sumThenReset();
            System.out.printf(
                "[stats] writes=%d  reads=%d  removes=%d  locks=%d  semAcquires=%d  increments=%d  errors=%d%n",
                w, r, d, l, a, c, e);
        }, 5, 5, TimeUnit.SECONDS);

        // ── main loop ─────────────────────────────────────────────────────────
        Random rng      = new Random();
        byte[] payload  = new byte[valueSize];
        boolean burst   = false;
        int     burstOps = 1;

        System.out.printf(
            "Starting: %d maps · %d locks · %d semaphores · %d counters · "
            + "%d keys · %dB values · %dms base cycle%n",
            maps.size(), lockList.size(), semList.size(), counterList.size(),
            keySpace, valueSize, intervalMs);

        while (true) {
            long cycleStart = System.currentTimeMillis();

            // ── burst-state transition (two-state Markov) ──────────────────────
            if (burst) {
                if (rng.nextDouble() < BURST_EXIT_PROB) {
                    burst    = false;
                    burstOps = 1;
                    System.out.println("[burst] exiting burst mode");
                }
            } else {
                if (rng.nextDouble() < BURST_ENTRY_PROB) {
                    burst    = true;
                    burstOps = BURST_OPS_MIN + rng.nextInt(BURST_OPS_MAX - BURST_OPS_MIN + 1);
                    System.out.printf("[burst] entering burst mode: %d ops/map%n", burstOps);
                }
            }

            // ── bulk-remove wave (only in normal mode) ─────────────────────────
            boolean removeWave = !burst && rng.nextDouble() < REMOVE_WAVE_PROB;
            if (removeWave) {
                System.out.printf("[wave] bulk-remove wave: ~%.0f%% of keys%n",
                    REMOVE_WAVE_KEY_FRAC * 100);
            }

            // ── CPMap operations ───────────────────────────────────────────────
            int opsPerMap = burst ? burstOps : 1;
            for (CPMap<String, byte[]> map : maps) {
                for (int i = 0; i < opsPerMap; i++) {
                    String key = pickKey(rng, keySpace);
                    try {
                        if (removeWave && rng.nextDouble() < REMOVE_WAVE_KEY_FRAC) {
                            map.remove(key);
                            removes.increment();
                        } else {
                            int roll = rng.nextInt(100);
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
                        }
                    } catch (Exception ex) {
                        errors.increment();
                        System.err.printf("map error %s key=%s: %s%n", map, key, ex.getMessage());
                    }
                }
            }

            // ── FencedLock: lock → work → unlock ──────────────────────────────
            for (FencedLock lock : lockList) {
                try {
                    lock.lock();
                    locks.increment();
                    Thread.sleep(lockHoldMs(rng));
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("lock error %s: %s%n", lock, ex.getMessage());
                } finally {
                    try { lock.unlock(); } catch (Exception ignored) {}
                }
            }

            // ── ISemaphore: acquire (variable count) → work → release ──────────
            for (ISemaphore sem : semList) {
                // 10% of attempts drain 2–4 permits instead of 1 to create
                // utilisation spikes in the Permit Utilisation Ratio panel.
                int toDrain = rng.nextDouble() < 0.10
                    ? 2 + rng.nextInt(3)
                    : 1;
                try {
                    if (sem.tryAcquire(toDrain, 10, TimeUnit.MILLISECONDS)) {
                        acquires.increment();
                        Thread.sleep(semHoldMs(rng));
                        sem.release(toDrain);
                    }
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("semaphore error %s: %s%n", sem, ex.getMessage());
                }
            }

            // ── IAtomicLong: increment ─────────────────────────────────────────
            for (IAtomicLong counter : counterList) {
                try {
                    counter.incrementAndGet();
                    increments.increment();
                } catch (Exception ex) {
                    errors.increment();
                    System.err.printf("counter error %s: %s%n", counter, ex.getMessage());
                }
            }

            long elapsed = System.currentTimeMillis() - cycleStart;
            long sleep   = intervalMs - elapsed;
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }
    }

    /**
     * Hot-spot key selection: {@value #HOT_RATIO} of accesses target the first
     * {@value #HOT_KEYS} keys; the rest are drawn uniformly from the full key space.
     */
    private static String pickKey(Random rng, int keySpace) {
        int hot = Math.min(HOT_KEYS, keySpace);
        int idx = rng.nextDouble() < HOT_RATIO
            ? rng.nextInt(hot)
            : rng.nextInt(keySpace);
        return "key-" + idx;
    }

    /**
     * Log-normal lock hold time: median ≈ 2 ms, 5% tail of 50–250 ms long holds.
     * Long holds simulate slow critical sections and drive lock-wait pressure.
     */
    private static long lockHoldMs(Random rng) {
        if (rng.nextDouble() < 0.05) {
            return 50 + rng.nextInt(200);
        }
        // log-normal: exp(μ=0.5, σ=0.7) → median ≈ 1.6 ms, mean ≈ 2.6 ms
        double ms = Math.exp(0.5 + 0.7 * rng.nextGaussian());
        return Math.max(1, Math.min((long) ms, 25));
    }

    /**
     * Log-normal semaphore hold time: shorter than lock; 3% tail of 20–100 ms.
     */
    private static long semHoldMs(Random rng) {
        if (rng.nextDouble() < 0.03) {
            return 20 + rng.nextInt(80);
        }
        double ms = Math.exp(0.3 + 0.5 * rng.nextGaussian());
        return Math.max(1, Math.min((long) ms, 15));
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
