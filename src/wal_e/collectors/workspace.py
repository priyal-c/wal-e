"""Workspace directory structure data collector."""

from __future__ import annotations

from typing import Any

from wal_e.collectors.base import BaseCollector


class WorkspaceCollector(BaseCollector):
    """Collects workspace directory structure and file metadata."""

    def collect(self) -> dict[str, Any]:
        """Collect workspace list at root, extract structure and file types."""
        findings: dict[str, Any] = {
            "directory_structure": [],
            "untitled_notebooks_count": 0,
            "file_types": {},
            "object_count": 0,
        }

        # Workspace list at root
        data, ok = self.run_api_call("/api/2.0/workspace/list?path=%2F")
        if ok and data:
            objects = data.get("objects", []) or []
            findings["object_count"] = len(objects)
            for obj in objects:
                if isinstance(obj, dict):
                    path = obj.get("path", "")
                    obj_type = obj.get("object_type", "UNKNOWN")
                    findings["directory_structure"].append({
                        "path": path,
                        "object_type": obj_type,
                        "language": obj.get("language"),
                    })
                    if obj_type not in findings["file_types"]:
                        findings["file_types"][obj_type] = 0
                    findings["file_types"][obj_type] += 1
                    if "Untitled" in path or path.endswith("Untitled"):
                        findings["untitled_notebooks_count"] += 1

        return findings
