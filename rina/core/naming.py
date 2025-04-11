# rina/core/naming.py

from typing import NewType

"""
Defines basic types for RINA naming conventions.

These are used for type hinting and semantic clarity throughout the codebase.
Using classes inheriting from base types allows for runtime type checking
with isinstance() while retaining most of the base type's behavior.
"""

# Keep NewType for types where isinstance against the specific type is not needed,
# or where runtime checks only compare against the base type (e.g., int).
from typing import NewType # Keep for PortId if isinstance checks are only against 'int'

class ApplicationName(str):
    """
    Represents an Application Name, identifying an application instance.
    Inherits from str for string-like behavior and runtime type checking.
    Intended to be unique within a relevant discovery scope.
    """
    pass

class DIFName(str):
    """
    Represents a DIF Name, identifying a Distributed IPC Facility.
    Inherits from str for string-like behavior and runtime type checking.
    """
    pass

# PortId can often remain a NewType if runtime checks primarily use isinstance(..., int).
# If you need to specifically check isinstance(..., PortId) at runtime, change this
# to 'class PortId(int): pass' as well.
PortId = NewType('PortId', int)
"""
A Port ID identifying a flow endpoint within an IPC Process.
Used for multiplexing and associating data with flow context.
(Using NewType for static analysis, runtime checks likely use 'int').
"""

class IPCProcessAddress(str):
    """
    Represents an address assigned to an IPC Process within a DIF during enrollment.
    May differ from the Application Name and be used for intra-DIF routing.
    Inherits from str for string-like behavior and runtime type checking.
    """
    pass


# Example usage (won't run here, just illustrative):
# app_name: ApplicationName = ApplicationName("finance_app/instance1")
# dif_name: DIFName = DIFName("corporate_lan_dif")
# flow_endpoint: PortId = PortId(5001)
# ipcp_addr: IPCProcessAddress = IPCProcessAddress("dif0_addr_123")

# Verification (runtime check example)
# name_example = ApplicationName("test_app")
# print(f"Is ApplicationName? {isinstance(name_example, ApplicationName)}") # Should print True
# print(f"Is str? {isinstance(name_example, str)}") # Should print True