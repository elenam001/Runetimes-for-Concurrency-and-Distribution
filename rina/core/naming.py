# rina/core/naming.py

from typing import NewType

"""
Defines basic types for RINA naming conventions.

These are used for type hinting and semantic clarity throughout the codebase.
In a more complex implementation, these could be classes with more structure
and validation logic.
"""

# An Application Name identifies an application instance seeking communication.
# It's intended to be unique within a certain scope (potentially globally,
# or at least unique enough for discovery within relevant DIFs).
ApplicationName = NewType('ApplicationName', str)

# A DIF Name identifies a Distributed IPC Facility.
DIFName = NewType('DIFName', str)

# A Port ID identifies a flow endpoint within an IPC Process. It's used
# to multiplex multiple flows over a single IPCP-to-IPCP channel (if needed)
# and to associate incoming data with the correct flow context.
PortId = NewType('PortId', int)

# An IPC Process Address might be assigned by a DIF during enrollment.
# It could be distinct from the Application Name and used for routing
# or identification strictly within that DIF's context. For simplicity,
# we might initially just use the ApplicationName.
IPCProcessAddress = NewType('IPCProcessAddress', str)


# Example usage (won't run here, just illustrative):
# app_name: ApplicationName = ApplicationName("finance_app/instance1")
# dif_name: DIFName = DIFName("corporate_lan_dif")
# flow_endpoint: PortId = PortId(5001)