"""
Agentic follow-up chat backed by two live MCP servers:
  - prom-mcp-server  (Prometheus tools)   — always connected
  - hz-mcp-server    (Hazelcast log tools) — connected when MCP_HZ_URL is set

Both servers are discovered at runtime via session.list_tools(). Tools are
merged into a single list forwarded to the LLM; each call is routed back to
the correct session by tool name.

Flow per user message:
  1. Open SSE connections to both MCP servers; discover and merge all tools.
  2. Run agentic loop:
       a. Stream LLM response — yield text chunks as SSE.
       b. If LLM requests tool calls, route each to the correct MCP session.
       c. Feed results back; repeat until LLM produces a final answer.
  3. Close connections.
"""

from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack
from typing import AsyncIterator

MAX_ITERATIONS = 10

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001")
MCP_HZ_URL     = os.environ.get("MCP_HZ_URL", "")   # optional; omit to disable log tools

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

CHAT_SYSTEM = """\
You are an expert SRE assistant for Hazelcast CP Subsystem.

You have access to:
1. A completed analysis of the cluster for a specific time window (provided below).
2. A live Prometheus MCP server — use its tools to dig deeper into any metric.
3. A live Hazelcast MCP server — use its tools to inspect member logs when the
   analysis or Prometheus data points to a specific issue on specific members.

## Prometheus tool guidance
- Check the completed analysis first; use Prometheus tools when finer granularity is needed.
- Call `prometheus_list_metrics` if you are unsure of the exact metric name.
- Write accurate PromQL for Hazelcast MC metrics (prefixes: hz_raft_*, hz_cp_*).
- Cite specific metric values and time ranges in your answers.

Common Hazelcast CP metric prefixes:
  hz_raft_*          — Raft consensus (term, commitIndex, lastApplied, memberCount, …)
  hz_cp_map_*        — CPMap size and storage bytes
  hz_cp_lock_*       — FencedLock acquire count and lock count
  hz_cp_semaphore_*  — ISemaphore available permits
  hz_cp_atomiclong_* — IAtomicLong values
  hz_cp_session_*    — CP session version and expiration time

## Hazelcast log tool guidance (token-efficient workflow)
When logs may help (e.g. elections, exceptions, timeout errors):

  Step 1 — ALWAYS call `hz_log_summary` first.
           It returns only counts (~50 tokens). Use it to identify which
           member(s) have WARN/ERROR lines before fetching any log content.

  Step 2 — Call `hz_get_logs` with:
           • `members` set to only the affected member(s) from step 1.
           • `level` = "WARN" (default) or "ERROR" to narrow scope.
           • `keywords` derived from Prometheus findings or the user's question
             (e.g. ["election", "WrongGroupException", "timeout", "cp-group"]).
             Keywords filter lines server-side — always set them when possible.
           • `max_lines` = 100 (default) unless the user needs more.

  Step 3 — Only call `hz_get_diagnostic_logs` if the user explicitly asks about
           diagnostics or if step 2 reveals an issue that needs deeper trace.

Never fetch logs for all members at once unless `hz_log_summary` shows errors
on multiple members — this wastes tokens on clean members.
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mcp_tool_to_anthropic(tool) -> dict:
    return {
        "name": tool.name,
        "description": tool.description or "",
        "input_schema": tool.inputSchema if hasattr(tool, "inputSchema") else {"type": "object", "properties": {}},
    }


def _mcp_tool_to_openai(tool) -> dict:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.inputSchema if hasattr(tool, "inputSchema") else {"type": "object", "properties": {}},
        },
    }


def _serialize_content(content: list) -> list[dict]:
    """Convert SDK response content blocks to plain dicts for the next API call."""
    out = []
    for block in content:
        if block.type == "text":
            out.append({"type": "text", "text": block.text})
        elif block.type == "tool_use":
            out.append({
                "type": "tool_use",
                "id": block.id,
                "name": block.name,
                "input": block.input,
            })
    return out


def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ---------------------------------------------------------------------------
# MCP session management
# ---------------------------------------------------------------------------

async def _open_mcp_session(stack: AsyncExitStack, url: str):
    """Open an SSE MCP session within an AsyncExitStack."""
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    read, write = await stack.enter_async_context(sse_client(url))
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _gather_tools(sessions: list) -> tuple[list, dict]:
    """
    Collect tools from all sessions and build a routing map.
    Returns (all_tools, {tool_name: session}).
    """
    from mcp import ClientSession
    tool_sessions: dict[str, ClientSession] = {}
    all_tools: list = []
    for session in sessions:
        for t in (await session.list_tools()).tools:
            tool_sessions[t.name] = session
            all_tools.append(t)
    return all_tools, tool_sessions


# ---------------------------------------------------------------------------
# Agentic streaming loop
# ---------------------------------------------------------------------------

async def chat_stream(
    messages: list[dict],
    analysis: str,
    prometheus_url: str,
    start: float,
    end: float,
    model: str,
) -> AsyncIterator[str]:
    """
    Run the MCP-backed agentic loop and yield SSE-formatted chunks.

    Events emitted:
      data: <json text chunk>   — LLM text token (default SSE message)
      event: tool_call          — an MCP tool is being invoked
      event: done               — stream finished
      event: error              — unrecoverable error
    """
    from datetime import datetime, timezone

    fmt = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    system = (
        CHAT_SYSTEM
        + f"\n\n## Analysis window\n- Start: {fmt(start)}\n- End:   {fmt(end)}\n"
        + f"\n## Completed Analysis\n{analysis}"
    )

    try:
        async with AsyncExitStack() as stack:
            # Always connect to the Prometheus MCP server
            prom_session = await _open_mcp_session(stack, f"{MCP_SERVER_URL}/sse")
            sessions = [prom_session]

            # Optionally connect to the Hazelcast log MCP server
            if MCP_HZ_URL:
                try:
                    hz_session = await _open_mcp_session(stack, f"{MCP_HZ_URL}/sse")
                    sessions.append(hz_session)
                except Exception as exc:
                    yield _sse("warning", f"Hazelcast MCP server unavailable: {exc}")

            all_tools, tool_sessions = await _gather_tools(sessions)

            if model.startswith("claude"):
                async for chunk in _loop_claude(model, system, messages, all_tools, tool_sessions):
                    yield chunk
            else:
                async for chunk in _loop_openai(model, system, messages, all_tools, tool_sessions):
                    yield chunk

    except Exception as exc:
        yield _sse("error", str(exc))


# ---------------------------------------------------------------------------
# Anthropic loop
# ---------------------------------------------------------------------------

async def _loop_claude(model, system, messages, mcp_tools, tool_sessions) -> AsyncIterator[str]:
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        yield _sse("error", "ANTHROPIC_API_KEY not set")
        return

    llm     = anthropic.AsyncAnthropic(api_key=api_key)
    tools   = [_mcp_tool_to_anthropic(t) for t in mcp_tools]
    history = list(messages)

    for _ in range(MAX_ITERATIONS):
        response_content = []

        async with llm.messages.stream(
            model=model,
            max_tokens=4096,
            system=system,
            tools=tools,
            messages=history,
        ) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and hasattr(event.delta, "text"):
                    yield f"data: {json.dumps(event.delta.text)}\n\n"
            final = await stream.get_final_message()
            response_content = final.content

        tool_uses = [b for b in response_content if b.type == "tool_use"]
        if not tool_uses:
            yield _sse("done", "[DONE]")
            return

        history.append({"role": "assistant", "content": _serialize_content(response_content)})

        tool_results = []
        for tu in tool_uses:
            yield _sse("tool_call", {"name": tu.name, "input": tu.input})
            result_text = await _call_mcp_tool(tool_sessions, tu.name, tu.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": result_text,
            })
        history.append({"role": "user", "content": tool_results})

    yield _sse("error", "Maximum tool iterations reached without a final answer.")


# ---------------------------------------------------------------------------
# OpenAI loop
# ---------------------------------------------------------------------------

async def _loop_openai(model, system, messages, mcp_tools, tool_sessions) -> AsyncIterator[str]:
    from openai import AsyncOpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        yield _sse("error", "OPENAI_API_KEY not set")
        return

    client  = AsyncOpenAI(api_key=api_key)
    tools   = [_mcp_tool_to_openai(t) for t in mcp_tools]
    history = [{"role": "system", "content": system}] + list(messages)

    for _ in range(MAX_ITERATIONS):
        text_buf: str = ""
        tool_calls_buf: dict[int, dict] = {}

        stream = await client.chat.completions.create(
            model=model,
            max_tokens=4096,
            stream=True,
            tools=tools,
            messages=history,
        )
        finish_reason = None
        async for chunk in stream:
            delta = chunk.choices[0].delta
            finish_reason = chunk.choices[0].finish_reason or finish_reason

            if delta.content:
                text_buf += delta.content
                yield f"data: {json.dumps(delta.content)}\n\n"

            if delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in tool_calls_buf:
                        tool_calls_buf[idx] = {"id": tc.id, "name": "", "arguments": ""}
                    if tc.id:
                        tool_calls_buf[idx]["id"] = tc.id
                    if tc.function.name:
                        tool_calls_buf[idx]["name"] += tc.function.name
                    if tc.function.arguments:
                        tool_calls_buf[idx]["arguments"] += tc.function.arguments

        if finish_reason != "tool_calls":
            yield _sse("done", "[DONE]")
            return

        tool_calls_list = [
            {
                "id": tool_calls_buf[idx]["id"],
                "type": "function",
                "function": {
                    "name": tool_calls_buf[idx]["name"],
                    "arguments": tool_calls_buf[idx]["arguments"],
                },
            }
            for idx in sorted(tool_calls_buf)
        ]
        assistant_msg: dict = {"role": "assistant", "tool_calls": tool_calls_list}
        if text_buf:
            assistant_msg["content"] = text_buf
        history.append(assistant_msg)

        for tc in tool_calls_list:
            name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except Exception:
                args = {}
            yield _sse("tool_call", {"name": name, "input": args})
            result_text = await _call_mcp_tool(tool_sessions, name, args)
            history.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result_text,
            })

    yield _sse("error", "Maximum tool iterations reached without a final answer.")


# ---------------------------------------------------------------------------
# Shared MCP tool executor — routes by tool name to the correct session
# ---------------------------------------------------------------------------

async def _call_mcp_tool(tool_sessions: dict, name: str, args: dict) -> str:
    session = tool_sessions.get(name)
    if session is None:
        return json.dumps({"error": f"No MCP session found for tool: {name}"})
    try:
        result = await session.call_tool(name, args)
        return "\n".join(c.text for c in result.content if hasattr(c, "text"))
    except Exception as exc:
        return json.dumps({"error": str(exc)})
