# TODO — CP Subsystem Monitoring Gaps

Items marked **[metric exists]** can be added to a dashboard today.
Items marked **[no metric]** require either a new data structure in the traffic generator or a future Hazelcast version that exposes additional metrics.

---

## Alerting

- [ ] **Fix recording rules** — `prometheus/rules/cp-subsystem.yml` uses non-existent V2 metric names (`hazelcast_raft_*`). Correct to actual V1 names (`hz_raft_*`). Recording rules currently produce no data.
- [ ] **Alert: log capacity low** — fire when `hz_raft_group_availableLogCapacity < 1000` for any group. Exhaustion causes writes to be rejected. `[metric exists]`
- [ ] **Alert: group member count below quorum** — fire when `hz_raft_group_memberCount < 2` for any group. Group becomes unavailable. `[metric exists]`
- [ ] **Alert: no leader elected** — detect when no series with `role="LEADER"` exists for a group for > 10 s. Indicates a stuck election. `[metric exists]`
- [ ] **Alert: semaphore permits exhausted** — fire when `hz_cp_semaphore_available == 0` for > N seconds. Clients will block. `[no metric yet — needs semaphore traffic]`
- [ ] **Alert: CP session about to expire** — fire when session expiration time is within 2× the session TTL and no recent heartbeat. `[no metric yet — needs session traffic]`
- [ ] **Alert: high lock contention** — fire when lock acquisition rate exceeds a threshold, or when lock wait time spikes. `[no metric yet — needs lock metrics]`

---

## Dashboards

### Raft Consensus

- [ ] **Append request backoff count** — number of times the leader backed off appending entries to followers (indicator of follower pressure). Metric: `hz_raft_group_appendRequestBackoffCount` — not yet confirmed to exist in MC exports.
- [ ] **Uncommitted entry count** — entries written to the leader log but not yet committed. When this approaches `uncommitted-entry-count-to-reject-new-appends` (200), new writes will be rejected. `[no metric confirmed]`
- [ ] **Apply rate vs commit rate** — overlay `rate(lastApplied[1m])` against `rate(commitIndex[1m])` per group. The gap is the application backlog, distinct from the replication lag. `[metric exists]`
- [ ] **Leader stability heatmap** — a heatmap showing which member holds leadership across all groups over time (requires Grafana transformation to pivot leader identity into a numeric series). `[metric exists]`
- [ ] **Raft snapshot size** — size in bytes of the last Raft snapshot per group. Large snapshots slow down follower catch-up. `[no metric confirmed]`
- [ ] **Follower catch-up progress** — when a follower restarts and must replay the log (or load a snapshot), track how far behind it is and how quickly it catches up. `[metric exists — derive from lastApplied lag]`

### Lock Contention

- [ ] **Lock wait time** — time between a lock request and lock acquisition. Requires `hz_cp_lock_waitTime` or similar. `[no metric confirmed]`
- [ ] **Lock hold time** — average duration a lock is held. Long hold times = contention amplifier. `[no metric confirmed]`
- [ ] **Lock reentrant count** — how often the same thread reacquires a lock it already holds. `[no metric confirmed]`
- [ ] **Failed lock attempts** — `tryLock()` calls that returned false (timed out). Indicates threads giving up under contention. `[no metric confirmed]`

### Semaphore

- [ ] **Semaphore acquire wait time** — time spent waiting for a permit. `[no metric confirmed]`
- [ ] **Semaphore drain events** — how often available permits hit 0. `[derive from hz_cp_semaphore_available]`
- [ ] **Permit utilisation ratio** — `(configured_permits − available) / configured_permits`. Shows how loaded the semaphore pool is. `[metric exists once semaphore traffic is active]`

### CP Sessions

- [ ] **Active session count** — total live CP sessions at any point. A growing count with no corresponding data structure activity indicates session leaks. `[no metric confirmed]`
- [ ] **Session heartbeat age** — time since the last heartbeat for each session. Sessions close to TTL without recent heartbeats are at risk of expiry. `[derive from hz_cp_session_version staleness]`
- [ ] **Session expiry rate** — how often sessions expire. Frequent expiry = clients not heartbeating (GC pauses, network issues, bugs). `[no metric confirmed]`

### IAtomicLong / IAtomicReference

- [ ] **CAS failure rate** — rate of failed compareAndSet operations. High rate = contention on a single counter/reference. `[no metric confirmed]`
- [ ] **Atomic operation latency** — p99 latency for get/set/compareAndSet. `[no metric confirmed]`

### CP Map

- [ ] **CPMap operation latency** — p50/p99 latency for get/set/remove per map. Available in MC but not currently exposed via Prometheus. `[no metric confirmed]`
- [ ] **CPMap max-size-mb utilisation** — `sizeBytes / max_size_mb_configured`. Alert before the map hits its limit. `[metric exists: hz_cp_map_sizeBytes]`
- [ ] **Per-key access distribution** — hot key detection. Not natively available via Prometheus; would require application-level instrumentation.

---

## Infrastructure & Pipeline

- [ ] **Fix Prometheus recording rules** to use actual `hz_raft_*` metric names (currently broken due to V2 name assumption).
- [ ] **Alertmanager integration** — route CP alerts to Slack/PagerDuty. Currently Prometheus has alerting rules but no Alertmanager configured.
- [ ] **Prometheus remote write** — forward metrics to a long-term storage backend (Thanos, Cortex, Mimir) for retention beyond 15 days.
- [ ] **Multi-cluster support** — parameterise the Prometheus scrape config to support multiple MC endpoints, each tagged with a different `mc_cluster` label. Both dashboards already support the `$cluster` variable.
- [ ] **Member-level Prometheus scraping** — scrape each `hz1:5701/metrics` … `hz5:5701/metrics` directly (requires enabling the `METRICS` endpoint group — removed in HZ 5.6; use JMX exporter or the new metrics REST endpoint instead). This allows per-member alerting independent of MC availability.
- [ ] **MC availability alert** — alert when the Management Center itself is unreachable (currently the scrape target going down silently starves all dashboards of data).
- [ ] **Traffic generator: configurable contention scenarios** — add env vars to tune lock hold time, semaphore permit count, and CAS contention level to simulate specific failure modes.
- [ ] **Chaos mode** — add a script that stops/starts individual HZ members on a schedule to observe leader re-election and quorum recovery in the dashboards.
