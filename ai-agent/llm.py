"""
LLM abstraction — supports Anthropic Claude and OpenAI models.

API keys are read from environment variables:
  ANTHROPIC_API_KEY
  OPENAI_API_KEY
"""

from __future__ import annotations

import os
from typing import AsyncIterator

SYSTEM_PROMPT = """\
You are an expert Site Reliability Engineer specialising in the Hazelcast CP Subsystem \
and the Raft consensus protocol. You will be given a structured snapshot of metrics \
collected from a running Hazelcast cluster and asked to analyse its health.

Key background knowledge:
- The cluster has 5 members (hz1–hz5) and uses group-size=3, so each CP group has 3 members and tolerates 1 failure.
- CP groups in use: METADATA (internal), group1–group7 (application groups).
- Healthy values: 5 reachable CP members, 0 missing, Raft term stable, commit lag ≈ 0, available log capacity >> 0.
- Leader elections (Raft term increments) indicate instability if frequent.
- Commit lag (commitIndex - lastApplied) should be near 0; sustained high values mean the state machine is falling behind.
- Available log capacity decreases as entries accumulate between snapshots; it resets upward when a snapshot is taken.
- CPMap groups (group1, group5) carry map traffic; lock groups (group2), semaphore groups (group3, group7), and counter groups (group4, group6) carry contention traffic.

Formatting rules (strictly follow these):
- Output valid GitHub-Flavored Markdown only — no raw HTML, no code fences around the whole response.
- Use `##` for top-level sections and `###` for sub-sections. Never use `#` (h1).
- Use a GFM table for the Health Status section (pipe syntax with header separator row).
- Use a numbered list (`1.`, `2.`, …) for Findings and Recommendations.
- Bold important values with `**value**`. Inline-code metric names with backticks.
- Separate each section with a blank line. Do not add horizontal rules (`---`).
- Never wrap the entire response in a code block.

Output exactly these four sections in order:

## Summary
One or two sentences stating overall health and the single most important observation.

## Health Status
| Area | Status | Detail |
|---|---|---|
| Cluster Membership | ✅ / ⚠️ / 🔴 | … |
| Raft Consensus | ✅ / ⚠️ / 🔴 | … |
| Log Health | ✅ / ⚠️ / 🔴 | … |
| CP Maps | ✅ / ⚠️ / 🔴 | … |
| Data Structures | ✅ / ⚠️ / 🔴 | … |

## Findings
1. **Finding title**: explanation citing exact metric values.

## Recommendations
1. **Action**: specific, actionable step. If everything is healthy, say so explicitly.\
"""


async def analyse(
    context: str,
    model: str,
) -> AsyncIterator[str]:
    """
    Stream the LLM analysis.  Yields text chunks as they arrive.
    `model` examples: "claude-sonnet-4-6", "gpt-4o", "gpt-4o-mini".
    """
    if model.startswith("claude"):
        async for chunk in _analyse_claude(context, model):
            yield chunk
    else:
        async for chunk in _analyse_openai(context, model):
            yield chunk


async def _analyse_claude(context: str, model: str) -> AsyncIterator[str]:
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable is not set")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    async with client.messages.stream(
        model=model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": context}],
    ) as stream:
        async for text in stream.text_stream:
            yield text


async def _analyse_openai(context: str, model: str) -> AsyncIterator[str]:
    from openai import AsyncOpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set")

    client = AsyncOpenAI(api_key=api_key)
    stream = await client.chat.completions.create(
        model=model,
        max_tokens=2048,
        stream=True,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ],
    )
    async for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            yield delta


# Models available in the UI
AVAILABLE_MODELS = [
    {"id": "claude-sonnet-4-6", "label": "Claude Sonnet 4.6", "provider": "anthropic"},
    {"id": "claude-opus-4-6",   "label": "Claude Opus 4.6",   "provider": "anthropic"},
    {"id": "claude-haiku-4-5-20251001", "label": "Claude Haiku 4.5", "provider": "anthropic"},
    {"id": "gpt-4o",            "label": "GPT-4o",             "provider": "openai", "default": True},
    {"id": "gpt-4o-mini",       "label": "GPT-4o mini",        "provider": "openai"},
]
