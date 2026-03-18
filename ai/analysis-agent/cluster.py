"""
Cluster context — describes the topology and workload roles of this CP cluster.

STATIC_CONTEXT is the authoritative fallback.  derive_context() enriches it
at analysis time by querying Prometheus for live values (member list, group
list, group size).  Group roles cannot be inferred from metrics alone, so they
always come from STATIC_CONTEXT.
"""

from __future__ import annotations

from prom import PrometheusClient

# ---------------------------------------------------------------------------
# Static context — update when cluster topology changes
# ---------------------------------------------------------------------------

STATIC_CONTEXT: dict = {
    "cp_members": ["hz1", "hz2", "hz3", "hz4", "hz5"],
    "cp_member_count": 5,
    "group_size": 3,
    "quorum_size": 2,
    "cp_groups": [
        "METADATA",
        "group1", "group2", "group3",
        "group4", "group5", "group6", "group7",
    ],
    "group_roles": {
        "cp_map":     ["group1", "group5"],
        "lock":       ["group2"],
        "semaphore":  ["group3", "group7"],
        "counter":    ["group4", "group6"],
    },
    "cp_map_max_size_mb": 20,
}


# ---------------------------------------------------------------------------
# Dynamic derivation
# ---------------------------------------------------------------------------

async def derive_context(prom: PrometheusClient) -> dict:
    """
    Query Prometheus to derive what can be observed at runtime, then merge
    with STATIC_CONTEXT (static values win for group_roles which are not
    observable from metrics).

    Fields derived dynamically:
      cp_members       — from mc_member labels on hz_raft_group_memberCount
      cp_member_count  — len(cp_members)
      group_size       — max(hz_raft_group_memberCount) across all series
      quorum_size      — group_size // 2 + 1
      cp_groups        — from name labels on hz_raft_group_term
      group_roles.cp_map — from group labels on hz_cp_map_size
    """
    ctx: dict = {
        **STATIC_CONTEXT,
        "group_roles": dict(STATIC_CONTEXT["group_roles"]),  # shallow copy
    }

    # ── CP members ────────────────────────────────────────────────────────
    try:
        results = await prom.query("hz_raft_group_memberCount")
        members = sorted({
            r["metric"].get("mc_member", "").split(":")[0]
            for r in results
            if r["metric"].get("mc_member")
        } - {""})
        if members:
            ctx["cp_members"] = members
            ctx["cp_member_count"] = len(members)
    except Exception:
        pass

    # ── Group size + quorum ───────────────────────────────────────────────
    try:
        results = await prom.query("max(hz_raft_group_memberCount)")
        if results:
            gs = int(float(results[0]["value"][1]))
            if gs > 0:
                ctx["group_size"] = gs
                ctx["quorum_size"] = gs // 2 + 1
    except Exception:
        pass

    # ── CP groups ─────────────────────────────────────────────────────────
    try:
        results = await prom.query("hz_raft_group_term")
        groups = sorted({
            r["metric"].get("name", "")
            for r in results
            if r["metric"].get("name")
        } - {""})
        if groups:
            ctx["cp_groups"] = groups
    except Exception:
        pass

    # ── CP-Map groups (observable from metric labels) ─────────────────────
    try:
        results = await prom.query("hz_cp_map_size")
        map_groups = sorted({
            r["metric"].get("group", "")
            for r in results
            if r["metric"].get("group")
        } - {""})
        if map_groups:
            ctx["group_roles"]["cp_map"] = map_groups
    except Exception:
        pass

    return ctx
