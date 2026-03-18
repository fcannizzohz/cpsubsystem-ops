"""
Agentic follow-up chat backed by a live Prometheus MCP server.

The MCP server (prometheus-mcp-server) is started as a subprocess for each
chat request using the stdio transport.  Tools are discovered at runtime via
session.list_tools() and forwarded to Claude as Anthropic tool definitions —
no hardcoding of tool names or schemas needed.

Flow per user message:
  1. Start prometheus-mcp-server subprocess (stdio transport).
  2. Initialise MCP session, discover available tools.
  3. Run agentic loop:
       a. Stream Claude response — yield text chunks as SSE.
       b. If Claude requests tool calls, execute them via the MCP session.
       c. Feed tool results back; repeat until Claude produces a final answer.
  4. Close MCP subprocess.
"""

from __future__ import annotations

import json
import os
from typing import AsyncIterator

MAX_ITERATIONS = 10

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8001")

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

CHAT_SYSTEM = """\
You are an expert SRE assistant for Hazelcast CP Subsystem.

You have access to:
1. A completed analysis of the cluster for a specific time window (provided below).
2. A live Prometheus MCP server — use its tools to dig deeper into any metric.

When answering follow-up questions:
- Check the completed analysis first; use Prometheus tools when finer granularity is needed.
- Use the metric-listing tool before writing queries if you are unsure of the exact metric name.
- Write accurate PromQL for Hazelcast MC metrics (prefixes: hz_raft_*, hz_cp_*).
- Cite specific metric values and time ranges in your answers.
- Be concise. Do not repeat the full analysis unless asked.

Common Hazelcast CP metric prefixes:
  hz_raft_*          — Raft consensus (term, commitIndex, lastApplied, memberCount, …)
  hz_cp_map_*        — CPMap size and storage bytes
  hz_cp_lock_*       — FencedLock acquire count and lock count
  hz_cp_semaphore_*  — ISemaphore available permits
  hz_cp_atomiclong_* — IAtomicLong values
  hz_cp_session_*    — CP session version and expiration time
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
      event: tool_call          — a Prometheus MCP tool is being invoked
      event: done               — stream finished
      event: error              — unrecoverable error
    """
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from datetime import datetime, timezone

    fmt = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    system = (
        CHAT_SYSTEM
        + f"\n\n## Analysis window\n- Start: {fmt(start)}\n- End:   {fmt(end)}\n"
        + f"\n## Completed Analysis\n{analysis}"
    )

    mcp_url = f"{MCP_SERVER_URL}/sse"

    try:
        async with sse_client(mcp_url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tools = (await session.list_tools()).tools

                if model.startswith("claude"):
                    async for chunk in _loop_claude(model, system, messages, mcp_tools, session):
                        yield chunk
                else:
                    async for chunk in _loop_openai(model, system, messages, mcp_tools, session):
                        yield chunk

    except Exception as exc:
        yield _sse("error", str(exc))


# ---------------------------------------------------------------------------
# Anthropic loop
# ---------------------------------------------------------------------------

async def _loop_claude(model, system, messages, mcp_tools, session) -> AsyncIterator[str]:
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        yield _sse("error", "ANTHROPIC_API_KEY not set")
        return

    llm   = anthropic.AsyncAnthropic(api_key=api_key)
    tools = [_mcp_tool_to_anthropic(t) for t in mcp_tools]
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
            result_text = await _call_mcp_tool(session, tu.name, tu.input)
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

async def _loop_openai(model, system, messages, mcp_tools, session) -> AsyncIterator[str]:
    from openai import AsyncOpenAI
    import asyncio

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        yield _sse("error", "OPENAI_API_KEY not set")
        return

    client = AsyncOpenAI(api_key=api_key)
    tools  = [_mcp_tool_to_openai(t) for t in mcp_tools]
    history = [{"role": "system", "content": system}] + list(messages)

    for _ in range(MAX_ITERATIONS):
        # Accumulate the full streamed response
        text_buf   = ""
        tool_calls_buf: dict[int, dict] = {}  # index → {id, name, arguments}

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

        # Build the assistant message with tool_calls
        tool_calls_list = []
        for idx in sorted(tool_calls_buf):
            tc = tool_calls_buf[idx]
            tool_calls_list.append({
                "id": tc["id"],
                "type": "function",
                "function": {"name": tc["name"], "arguments": tc["arguments"]},
            })
        assistant_msg: dict = {"role": "assistant", "tool_calls": tool_calls_list}
        if text_buf:
            assistant_msg["content"] = text_buf
        history.append(assistant_msg)

        # Execute each tool via MCP and append results
        for tc in tool_calls_list:
            name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except Exception:
                args = {}
            yield _sse("tool_call", {"name": name, "input": args})
            result_text = await _call_mcp_tool(session, name, args)
            history.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result_text,
            })

    yield _sse("error", "Maximum tool iterations reached without a final answer.")


# ---------------------------------------------------------------------------
# Shared MCP tool executor
# ---------------------------------------------------------------------------

async def _call_mcp_tool(session, name: str, args: dict) -> str:
    try:
        result = await session.call_tool(name, args)
        return "\n".join(c.text for c in result.content if hasattr(c, "text"))
    except Exception as exc:
        return json.dumps({"error": str(exc)})
