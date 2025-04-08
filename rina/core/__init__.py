"""
Core components of the RINA implementation.

This package contains the fundamental building blocks like DIF, IPCProcess, Flow, Naming, and PDU structures.
"""

from .dif import DIF
from .ipcp import IPCProcess
from .flow import Flow
from .naming import ApplicationName, DIFName
from .pdu import PDU

CORE_VERSION = "0.1.0"

__all__ = [
    "DIF",
    "IPCProcess",
    "Flow",
    "ApplicationName",
    "DIFName",
    "PDU",
]

print("Initializing RINA Core Package")