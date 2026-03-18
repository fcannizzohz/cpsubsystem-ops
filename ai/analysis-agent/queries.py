"""
Curated PromQL queries for Hazelcast CP Subsystem analysis.

Instant queries  → snapshot at the end of the analysis window.
Range queries    → time-series over the window, summarised server-side before
                   being sent to the LLM.
"""

from dataclasses import dataclass


@dataclass
class InstantQuery:
    name: str
    query: str
    description: str
    healthy_hint: str = ""


@dataclass
class RangeQuery:
    name: str
    query: str
    description: str
    unit: str = ""
    healthy_hint: str = ""


# ---------------------------------------------------------------------------
# Instant queries — current state at end of the analysis period
# ---------------------------------------------------------------------------

INSTANT_QUERIES: list[InstantQuery] = [
    InstantQuery(
        name="reporting_members",
        query="count(count by (mc_member)(hz_raft_group_term))",
        description="Members actively sending metrics to Management Center right now",
        healthy_hint="Must equal cp_member_count; any shortfall = a member is silent (likely down)",
    ),
    InstantQuery(
        name="reachable_cp_members",
        query="max(hz_raft_metadata_activeMembers) - max(hz_raft_missingMembers)",
        description="Reachable CP members per CP subsystem self-report (may lag after a crash)",
        healthy_hint="5 for a 5-node cluster; < 3 risks quorum loss",
    ),
    InstantQuery(
        name="missing_cp_members",
        query="max(hz_raft_missingMembers)",
        description="CP members flagged unreachable by the CP subsystem (may lag after a crash)",
        healthy_hint="0; any value > 0 is a warning",
    ),
    InstantQuery(
        name="total_cp_groups",
        query="max(hz_raft_metadata_groups)",
        description="Total CP groups tracked by METADATA",
        healthy_hint="Stable; increases only when a new group is created",
    ),
    InstantQuery(
        name="terminated_raft_groups",
        query="max(hz_raft_terminatedRaftNodeGroupIds)",
        description="Terminated Raft group IDs",
        healthy_hint="0; any value > 0 indicates a destroyed group",
    ),
    InstantQuery(
        name="group_member_counts",
        query="hz_raft_group_memberCount",
        description="Members per CP group (per member view)",
        healthy_hint="3 per group; 2 = degraded, 1 = group unavailable",
    ),
    InstantQuery(
        name="available_log_capacity",
        query="min by (name)(hz_raft_group_availableLogCapacity)",
        description="Minimum remaining Raft log capacity per group",
        healthy_hint="Approaches 0 as log fills; writes rejected at 0; alert at < 1000",
    ),
    InstantQuery(
        name="commit_lag_current",
        query="hz_raft_group_commitIndex - hz_raft_group_lastApplied",
        description="Current commit lag (commitIndex - lastApplied) per group/member",
        healthy_hint="Near 0 in steady state; persistent high value = state machine falling behind",
    ),
    InstantQuery(
        name="raft_terms",
        query="max by (name)(hz_raft_group_term)",
        description="Current Raft term per CP group (increments on each election)",
        healthy_hint="Flat over time = stable; step-ups indicate elections",
    ),
    InstantQuery(
        name="cp_map_sizes",
        query="sum by (name, group)(hz_cp_map_size)",
        description="Entry count per CPMap",
        healthy_hint="Depends on workload; watch for unexpected growth or drops",
    ),
    InstantQuery(
        name="cp_map_storage_bytes",
        query="sum by (name)(hz_cp_map_sizeBytes)",
        description="Storage bytes per CPMap",
        healthy_hint="Grows with entry count; alert if approaching max-size-mb",
    ),
    InstantQuery(
        name="raft_nodes_per_member",
        query="max by (mc_member)(hz_raft_nodes)",
        description="Number of Raft nodes hosted by each member",
        healthy_hint="Should be equal across members (~3 for group-size=3 with 8 groups)",
    ),
    # ── Data structures ───────────────────────────────────────────────────
    InstantQuery(
        name="semaphore_available",
        query="hz_cp_semaphore_available",
        description="Available permits per ISemaphore (current snapshot)",
        healthy_hint="Equal to initial permit count = idle; 0 = exhausted, clients will block",
    ),
    InstantQuery(
        name="lock_hold_count",
        query="hz_cp_lock_lockCount",
        description="Current concurrent lock holders per FencedLock",
        healthy_hint="0 = idle; 1 = one holder; >1 unexpected for non-reentrant FencedLock",
    ),
    InstantQuery(
        name="atomiclong_values",
        query="hz_cp_atomiclong_value",
        description="Current value of each IAtomicLong counter",
        healthy_hint="Monotonically increasing under load; use rate for throughput signal",
    ),
]


# ---------------------------------------------------------------------------
# Range queries — time-series over the analysis period, summarised server-side
# ---------------------------------------------------------------------------

RANGE_QUERIES: list[RangeQuery] = [
    RangeQuery(
        name="leader_elections",
        query="changes(hz_raft_group_term[5m])",
        description="Leader elections per CP group in 5-minute windows",
        healthy_hint="0 throughout = perfectly stable; occasional 1s = normal; frequent spikes = instability",
    ),
    RangeQuery(
        name="commit_lag_over_time",
        query="hz_raft_group_commitIndex - hz_raft_group_lastApplied",
        description="Replication lag (commitIndex - lastApplied) per group/member over time",
        unit="log entries",
        healthy_hint="Should hover near 0; sustained lag > 100 = follower issue",
    ),
    RangeQuery(
        name="commit_rate",
        query="rate(hz_raft_group_commitIndex[1m])",
        description="Raft commit rate per CP group",
        unit="entries/s",
        healthy_hint="Proportional to write load; drops to 0 under no traffic; large variance = bursty writes",
    ),
    RangeQuery(
        name="missing_members_over_time",
        query="max(hz_raft_missingMembers)",
        description="Missing CP members over time",
        healthy_hint="0 throughout = all members healthy; any non-zero = member was absent",
    ),
    RangeQuery(
        name="log_capacity_over_time",
        query="min by (name)(hz_raft_group_availableLogCapacity)",
        description="Minimum available Raft log capacity per group over time",
        healthy_hint="Decreasing trend without recovery = snapshots not keeping up",
    ),
    RangeQuery(
        name="cp_map_entry_trend",
        query="sum by (name, group)(hz_cp_map_size)",
        description="CPMap entry count trend over time",
        healthy_hint="Stable or expected growth; sudden drops = eviction or destroy",
    ),
    RangeQuery(
        name="apply_rate",
        query="rate(hz_raft_group_lastApplied[1m])",
        description="State-machine apply rate per CP group",
        unit="entries/s",
        healthy_hint="Should track commit rate closely; divergence = application backlog building",
    ),
    # ── Data structures ───────────────────────────────────────────────────
    RangeQuery(
        name="semaphore_permits_over_time",
        query="hz_cp_semaphore_available",
        description="Available ISemaphore permits over time",
        unit="permits",
        healthy_hint="Stable near initial count = low contention; drops + recovery = healthy burst; sustained at 0 = exhaustion",
    ),
    RangeQuery(
        name="lock_acquire_rate",
        query="rate(hz_cp_lock_acquireCount[1m])",
        description="FencedLock acquisition rate per lock",
        unit="acquisitions/s",
        healthy_hint="Proportional to workload; zero = no lock activity; spike = burst or contention",
    ),
    RangeQuery(
        name="atomiclong_increment_rate",
        query="rate(hz_cp_atomiclong_value[1m])",
        description="IAtomicLong increment rate per counter",
        unit="increments/s",
        healthy_hint="Proportional to counter workload; zero = no counter activity",
    ),
    RangeQuery(
        name="session_heartbeat_rate",
        query="rate(hz_cp_session_version[30s])",
        description="CP session heartbeat rate (version increments per second)",
        unit="heartbeats/s",
        healthy_hint="Non-zero = sessions are active and heartbeating; zero = no active sessions or sessions stalled",
    ),
]
