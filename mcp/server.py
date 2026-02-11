"""
WAL-E MCP Server

Exposes WAL-E assessment tools via the Model Context Protocol for AI Dev Kit
and other MCP clients. Uses FastMCP if available, otherwise a minimal JSON-RPC stub.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root and src are on path when running as script
_project_root = Path(__file__).resolve().parent.parent
_src = _project_root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _create_fastmcp_server():
    """Create and return FastMCP server with WAL-E tools."""
    from fastmcp import FastMCP
    from mcp.tools import wal_e_assess, wal_e_collect, wal_e_report, wal_e_score, wal_e_validate

    mcp = FastMCP(
        name="WAL-E",
        description="Well-Architected Lakehouse Evaluator - Assess Databricks workspaces against WAL best practices",
    )

    @mcp.tool(name="wal_e_assess", description="Run a full WAL-E assessment against a Databricks workspace.")
    def _assess(profile: str = "DEFAULT", output_dir: str = "./wal-e-assessment") -> dict:
        return wal_e_assess(profile=profile, output_dir=output_dir)

    @mcp.tool(name="wal_e_collect", description="Collect workspace data only (no scoring or reporting).")
    def _collect(profile: str = "DEFAULT") -> dict:
        return wal_e_collect(profile=profile)

    @mcp.tool(name="wal_e_score", description="Score assessment from cached collected data.")
    def _score(collected_data_path: str) -> dict:
        return wal_e_score(collected_data_path=collected_data_path)

    @mcp.tool(name="wal_e_report", description="Generate reports from cached assessment data.")
    def _report(assessment_path: str, format: str = "md") -> dict:
        return wal_e_report(assessment_path=assessment_path, format=format)

    @mcp.tool(name="wal_e_validate", description="Validate workspace access and connectivity.")
    def _validate(profile: str = "DEFAULT") -> dict:
        return wal_e_validate(profile=profile)

    return mcp


def _create_stub_server():
    """Create a minimal JSON-RPC stdio server when FastMCP is not available."""
    import json

    from mcp.tools import wal_e_assess, wal_e_collect, wal_e_report, wal_e_score, wal_e_validate

    TOOLS = {
        "wal_e_assess": wal_e_assess,
        "wal_e_collect": wal_e_collect,
        "wal_e_score": wal_e_score,
        "wal_e_report": wal_e_report,
        "wal_e_validate": wal_e_validate,
    }

    def handle_request(line: str) -> str | None:
        try:
            req = json.loads(line)
            method = req.get("method")
            params = req.get("params", {})

            if method == "initialize":
                return json.dumps({
                    "jsonrpc": "2.0",
                    "id": req.get("id"),
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "WAL-E", "version": "0.1.0"},
                    },
                })
            if method == "tools/call":
                name = params.get("name", "")
                args = params.get("arguments", {})
                fn = TOOLS.get(name)
                if fn:
                    result = fn(**args)
                    return json.dumps({
                        "jsonrpc": "2.0",
                        "id": req.get("id"),
                        "result": {"content": [{"type": "text", "text": json.dumps(result)}]},
                    })
            return json.dumps({"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32601, "message": "Method not found"}})
        except Exception as e:
            return json.dumps({"jsonrpc": "2.0", "id": req.get("id", None), "error": {"code": -32603, "message": str(e)}})

    def run():
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            resp = handle_request(line)
            if resp:
                print(resp, flush=True)

    return run


def main() -> None:
    """Run the MCP server."""
    try:
        mcp = _create_fastmcp_server()
        mcp.run()
    except ImportError:
        # Fallback: minimal stdio JSON-RPC
        run = _create_stub_server()
        run()


if __name__ == "__main__":
    main()
