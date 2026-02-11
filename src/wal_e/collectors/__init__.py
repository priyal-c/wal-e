"""Data collectors for workspace assessment."""

from wal_e.collectors.auth import AuthCollector
from wal_e.collectors.base import BaseCollector
from wal_e.collectors.compute import ComputeCollector
from wal_e.collectors.governance import GovernanceCollector
from wal_e.collectors.operations import OperationsCollector
from wal_e.collectors.security import SecurityCollector
from wal_e.collectors.workspace import WorkspaceCollector

__all__ = [
    "AuthCollector",
    "BaseCollector",
    "ComputeCollector",
    "GovernanceCollector",
    "OperationsCollector",
    "SecurityCollector",
    "WorkspaceCollector",
]
