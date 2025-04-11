# rina/management/layer_manager.py

import asyncio
import logging
from typing import Dict, Optional, Any, TYPE_CHECKING, Union

# Use forward references or TYPE_CHECKING for type hints
if TYPE_CHECKING:
    from ..core.dif import DIF
    # Ensure IPCProcessState is imported if used
    from ..core.ipcp import IPCProcess, ApplicationHandle, IPCProcessState
    from ..core.naming import ApplicationName, DIFName
    from ..core.pdu import PDU
    from .policy import PolicyManager
    from ..interfaces.network_interface import NetworkInterface, UnderlyingAddress

# Import necessary types directly for runtime use
from ..core.naming import ApplicationName, DIFName
from ..core.ipcp import IPCProcess # Needed for isinstance
from ..interfaces.network_interface import NetworkInterface # Needed for isinstance

# Define type alias if not already defined elsewhere accessible
try:
    from ..interfaces.network_interface import UnderlyingAddress
except ImportError:
    UnderlyingAddress = Any


log = logging.getLogger(__name__)

# --- Placeholder Address Resolution ---
# Hardcoded placeholder addresses for simulation.
# Replace with real discovery/configuration/routing logic.
# Assumes UDP setup where IPCPs/Services share or map to specific UDP ports.
PLACEHOLDER_ADDRESS_MAP = {
    # ApplicationName: (IP, Port)

    # --- DIF 0 ('dif0') Components (Interface on Port 8000) ---
    # Node IPCP handling all traffic for the interface
    ApplicationName("dif0-node"): ("127.0.0.1", 8000),

    # Management services for DIF 0, conceptually handled by dif0-node
    ApplicationName("dif0_enrollment_service"): ("127.0.0.1", 8000),
    ApplicationName("dif0_flow_allocator_service"): ("127.0.0.1", 8000),

    # --- DIF 1 ('normal_dif') Components (Using DIF0 transport) ---
    ApplicationName("normal_dif_enrollment_service"): ("127.0.0.1", 8000),
    ApplicationName("normal_dif_flow_allocator_service"): ("127.0.0.1", 8000),

    # Application IPCPs remain unchanged
    ApplicationName("App1"): ("127.0.0.1", 8000),
    ApplicationName("App2"): ("127.0.0.1", 8000),
}


class LayerManager:
    """
    Manages the creation, configuration, and lifecycle of DIFs and IPCPs.
    Handles layering, provides placeholder address resolution, and connects
    interfaces to bottom-layer IPCPs.
    """

    def __init__(self):
        self._difs: Dict[DIFName, 'DIF'] = {}
        self._ipcps: Dict[ApplicationName, 'IPCProcess'] = {}
        self._interfaces: Dict[str, 'NetworkInterface'] = {}
        self._difs_lock = asyncio.Lock()
        self._ipcps_lock = asyncio.Lock()
        self._interfaces_lock = asyncio.Lock()

        # Basic routing/addressing info (Placeholder)
        self._address_map = dict(PLACEHOLDER_ADDRESS_MAP)

        log.info("LayerManager initialized.")

    # --- Creation and Retrieval ---
    async def create_dif(self,
                         dif_name: DIFName,
                         config: Optional[Dict[str, Any]] = None,
                         policy_manager: Optional['PolicyManager'] = None) -> Optional['DIF']:
        """Creates a new DIF instance and registers it."""
        try: from ..core.dif import DIF
        except ImportError: from rina.core.dif import DIF # Adjust path as needed

        async with self._difs_lock:
            if dif_name in self._difs:
                log.warning(f"DIF '{dif_name}' already exists.")
                return self._difs[dif_name]

            log.info(f"Creating DIF '{dif_name}'...")
            new_dif = DIF(
                dif_name=dif_name, layer_manager=self,
                policy_manager=policy_manager, config=config
            )
            new_dif.management_address = ApplicationName(f"{dif_name}_management")
            self._difs[dif_name] = new_dif
            log.info(f"DIF '{dif_name}' created with mgmt address '{new_dif.management_address}'.")
            return new_dif

    async def get_dif(self, dif_name: DIFName) -> Optional['DIF']:
        """Retrieves a DIF instance by name."""
        async with self._difs_lock:
            return self._difs.get(dif_name)

    async def create_ipcp(self,
                          app_name: ApplicationName,
                          target_dif_name: DIFName,
                          app_handle: Optional['ApplicationHandle'] = None) -> Optional['IPCProcess']:
        """Creates an IPC Process instance intended to join the target DIF."""
        try: from ..core.ipcp import IPCProcess
        except ImportError: from rina.core.ipcp import IPCProcess # Adjust path as needed

        parent_dif = await self.get_dif(target_dif_name)
        if not parent_dif:
            log.error(f"Cannot create IPCP '{app_name}', target DIF '{target_dif_name}' not found.")
            return None

        async with self._ipcps_lock:
            if app_name in self._ipcps:
                log.warning(f"IPCP '{app_name}' already exists.")
                return self._ipcps[app_name]

            log.info(f"Creating IPCP '{app_name}' targeting DIF '{target_dif_name}'...")
            new_ipcp = IPCProcess(
                app_name=app_name, parent_dif=parent_dif,
                layer_manager=self, app_handle=app_handle
            )
            self._ipcps[app_name] = new_ipcp
            log.info(f"IPCP '{app_name}' created.")
            return new_ipcp

    async def get_ipcp(self, app_name: ApplicationName) -> Optional['IPCProcess']:
        """Retrieves an IPCP instance by Application Name."""
        async with self._ipcps_lock:
            return self._ipcps.get(app_name)

    async def add_network_interface(self, interface_id: str, interface: NetworkInterface):
         """Registers a low-level network interface."""
         async with self._interfaces_lock:
              if interface_id in self._interfaces:
                   log.warning(f"Network interface '{interface_id}' already registered.")
                   return
              self._interfaces[interface_id] = interface
              log.info(f"Network interface '{interface_id}' added ({type(interface).__name__}).")

    # --- Layering Configuration ---
    async def stack_dif_over(self, upper_dif_name: DIFName, lower_layer_id: str):
        """
        Configures layering: Assigns underlying transport to IPCPs in the upper DIF,
        configures the upper DIF's management transport, and registers handlers
        with the network interface if applicable using a 'node IPCP' convention.
        """
        log.info(f"Attempting to stack DIF '{upper_dif_name}' over '{lower_layer_id}'...")

        upper_dif = await self.get_dif(upper_dif_name)
        if not upper_dif:
            log.error(f"Cannot stack, upper DIF '{upper_dif_name}' not found.")
            return

        # Find lower layer (DIF or Interface)
        lower_dif = await self.get_dif(DIFName(lower_layer_id))
        lower_interface: Optional['NetworkInterface'] = None
        if not lower_dif:
             async with self._interfaces_lock:
                  lower_interface = self._interfaces.get(lower_layer_id)

        if not lower_dif and not lower_interface:
             log.error(f"Cannot stack, lower layer '{lower_layer_id}' (DIF or Interface) not found.")
             return

        # Determine the single transport mechanism for this stacking relationship
        assigned_transport: Optional[Union['IPCProcess', 'NetworkInterface']] = None
        node_ipcp_for_interface: Optional['IPCProcess'] = None # Specific IPCP handling interface if lower is NI

        if lower_dif:
            # Simplification: Assume a single 'node representative' IPCP in the lower DIF
            node_ipcp_name = ApplicationName(f"{lower_layer_id}-node") # Convention
            node_ipcp = await self.get_ipcp(node_ipcp_name)
            if not node_ipcp:
                 log.error(f"Cannot stack over DIF '{lower_layer_id}'. Node representative IPCP '{node_ipcp_name}' not found.")
                 return
            assigned_transport = node_ipcp
            log.info(f"Stacking: Using IPCP '{node_ipcp_name}' in DIF '{lower_layer_id}' as transport.")
        elif lower_interface:
            assigned_transport = lower_interface
            log.info(f"Stacking: Using Network Interface '{lower_layer_id}' as transport.")

        # Assign transport to relevant IPCPs & find node IPCP if using interface
        async with self._ipcps_lock:
            ipcps_in_upper_dif = [ipcp for ipcp in self._ipcps.values() if ipcp.parent_dif == upper_dif]

            if not ipcps_in_upper_dif and isinstance(assigned_transport, NetworkInterface):
                 log.warning(f"Stacking DIF '{upper_dif_name}' over interface '{lower_layer_id}', but no IPCPs targeting this DIF exist yet to assign transport or register handler.")
                 # Handler registration will be skipped below

            for ipcp in ipcps_in_upper_dif:
                if isinstance(assigned_transport, IPCProcess):
                    ipcp.add_underlying_ipcp(DIFName(lower_layer_id), assigned_transport)
                elif isinstance(assigned_transport, NetworkInterface):
                    ipcp.assign_network_interface(lower_layer_id, assigned_transport)
                    # Identify the node IPCP responsible for this interface (based on convention)
                    # Assumes only one node IPCP per DIF level handles the interface link
                    node_convention_name = ApplicationName(f"{upper_dif_name}-node")
                    if ipcp.app_name == node_convention_name:
                         if node_ipcp_for_interface:
                              log.warning(f"Multiple IPCPs match node convention '{node_convention_name}' for interface '{lower_layer_id}'. Using first one found: '{node_ipcp_for_interface.app_name}'")
                         else:
                              node_ipcp_for_interface = ipcp

        # --- Register Handler with Interface ---
        if isinstance(assigned_transport, NetworkInterface):
            if node_ipcp_for_interface:
                log.info(f"Registering handler: Interface '{lower_layer_id}' ({assigned_transport.local_address}) -> IPCP '{node_ipcp_for_interface.app_name}'.receive_pdu")
                try:
                    assigned_transport.register_handler(
                        assigned_transport.local_address,
                        node_ipcp_for_interface.receive_pdu
                    )
                except Exception as e:
                    log.error(f"Failed to register handler for interface '{lower_layer_id}': {e}", exc_info=True)
            elif ipcps_in_upper_dif: # Interface exists, IPCPs exist, but no node IPCP found
                 log.error(f"Could not identify node IPCP (e.g., '{upper_dif_name}-node') to register handler for Interface '{lower_layer_id}'. Incoming packets will likely be lost.")
            # If no IPCPs exist yet, handler cannot be registered now.

        # --- Configure Management Transport for the Upper DIF ---
        if assigned_transport:
            # If stacking over interface, the identified node IPCP acts as the proxy transport for DIF mgmt
            if isinstance(assigned_transport, NetworkInterface) and node_ipcp_for_interface:
                upper_dif.management_transport = node_ipcp_for_interface
                log.info(f"Configured management transport for DIF '{upper_dif_name}' via proxy IPCP '{node_ipcp_for_interface.app_name}'.")
            elif isinstance(assigned_transport, IPCProcess):
                upper_dif.management_transport = assigned_transport # Use lower node IPCP directly
                log.info(f"Configured management transport for DIF '{upper_dif_name}' via underlying IPCP '{assigned_transport.app_name}'.")
            else: # Interface exists but no node IPCP identified? Fallback
                upper_dif.management_transport = assigned_transport
                log.warning(f"Management transport for DIF '{upper_dif_name}' set directly to Interface '{lower_layer_id}'.")
        else:
             log.error(f"Failed to determine transport for stacking '{upper_dif_name}'. DIF management transport NOT SET.")


    # --- Address Resolution (Placeholders) ---
    async def resolve_app_to_underlying_address(self,
                                               app_name: ApplicationName,
                                               context_ipcp: Optional['IPCProcess'] = None) -> Optional[Any]:
        """
        Resolves App Name -> Lowest Layer Address (Placeholder).
        """
        log.debug(f"Resolving underlying address for AppName: {app_name}")
        # TODO: Implement real recursive resolution based on DIF structure & routing.

        address = self._address_map.get(app_name)
        if address:
             log.debug(f"Resolved '{app_name}' to placeholder address: {address}")
             return address
        else:
             log.warning(f"Failed to resolve placeholder address for AppName '{app_name}'.")
             return None

    async def resolve_service_to_underlying_address(self,
                                                   service_name: ApplicationName,
                                                   target_dif: 'DIF',
                                                   context_ipcp: Optional['IPCProcess'] = None) -> Optional[Any]:
        """
        Resolves DIF Service Name -> Lowest Layer Address (Placeholder).
        """
        log.debug(f"Resolving underlying address for Service: {service_name} in DIF '{target_dif.dif_name}'")

        # Construct specific key (e.g., "dif0_enroll_service")
        service_parts = str(service_name).split('_')
        base_service_type = "_".join(service_parts[1:]) if len(service_parts) > 1 else str(service_name)
        specific_key = ApplicationName(f"{target_dif.dif_name}_enrollment_service")
        if specific_key not in self._address_map:
            specific_key = ApplicationName(f"{target_dif.dif_name}_enroll_service")
    
        log.debug(f"Constructed specific_key: {specific_key!r}")
        log.debug(f"Current self._address_map keys: {[repr(k) for k in self._address_map.keys()]}")
    
        log.debug(f"Trying specific service key: {specific_key}")
        address = self._address_map.get(specific_key)

        if not address: # Fallback to original name
             log.debug(f"Trying original service key: {service_name}")
             address = self._address_map.get(service_name)

        if address:
             log.debug(f"Resolved service '{service_name}' in DIF '{target_dif.dif_name}' to placeholder address: {address}")
             return address
        else:
             log.warning(f"Failed to resolve placeholder address for service '{service_name}' in DIF '{target_dif.dif_name}' (tried keys: {specific_key}, {service_name}). Check PLACEHOLDER_ADDRESS_MAP.")
             return None

    # --- Lifecycle Management ---
    async def enroll_ipcp(self, app_name: ApplicationName) -> bool:
        """Finds an IPCP and initiates its enrollment process."""
        ipcp = await self.get_ipcp(app_name)
        if not ipcp:
            log.error(f"Cannot enroll, IPCP '{app_name}' not found.")
            return False

        try: from ..core.ipcp import IPCProcessState
        except ImportError: from rina.core.ipcp import IPCProcessState # Adjust path

        if ipcp.state != IPCProcessState.UNENROLLED:
             log.warning(f"IPCP '{app_name}' not UNENROLLED ({ipcp.state}). Cannot enroll.")
             return False

        if not ipcp._underlying_ipcps and not ipcp.network_interface:
             log.error(f"Cannot enroll IPCP '{app_name}': No underlying transport configured.")
             return False

        log.info(f"LayerManager requesting enrollment for IPCP '{app_name}'...")
        success = await ipcp.enroll()
        return success

    async def start_all(self):
        """Starts interfaces, then DIFs, then IPCPs concurrently."""
        log.info("Starting all Network Interfaces...")
        async with self._interfaces_lock:
            tasks = [asyncio.create_task(inf.start(), name=f"start_if_{inf_id}")
                     for inf_id, inf in self._interfaces.items() if hasattr(inf, 'start')]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Error logging for interface start failures
            interfaces_list = list(self._interfaces.keys())
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                     log.error(f"Error starting Interface '{interfaces_list[i]}': {res}", exc_info=res)

        log.info("Starting all DIFs...")
        async with self._difs_lock:
            tasks = [asyncio.create_task(dif.start(), name=f"start_dif_{dif_name}")
                     for dif_name, dif in self._difs.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Error logging for DIF start failures
            difs_list = list(self._difs.keys())
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                     log.error(f"Error starting DIF '{difs_list[i]}': {res}", exc_info=res)

        log.info("Starting all IPCPs...")
        async with self._ipcps_lock:
            tasks = [asyncio.create_task(ipcp.start(), name=f"start_ipcp_{app_name}")
                     for app_name, ipcp in self._ipcps.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Error logging for IPCP start failures
            ipcps_list = list(self._ipcps.keys())
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                     log.error(f"Error starting IPCP '{ipcps_list[i]}': {res}", exc_info=res)

        log.info("LayerManager start_all sequence complete.")

    async def stop_all(self):
        """Stops IPCPs, then DIFs, then interfaces concurrently."""
        log.info("Stopping all IPCPs...")
        async with self._ipcps_lock:
            tasks = [asyncio.create_task(ipcp.stop(), name=f"stop_ipcp_{name}")
                     for name, ipcp in self._ipcps.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log exceptions during IPCP stop
            ipcps_names = list(self._ipcps.keys())
            for i, res in enumerate(results):
                 if isinstance(res, Exception):
                      log.error(f"Error stopping IPCP '{ipcps_names[i]}': {res}", exc_info=res)
        self._ipcps.clear()

        log.info("Stopping all DIFs...")
        async with self._difs_lock:
            tasks = [asyncio.create_task(dif.stop(), name=f"stop_dif_{name}")
                     for name, dif in self._difs.items()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log exceptions during DIF stop
            difs_names = list(self._difs.keys())
            for i, res in enumerate(results):
                 if isinstance(res, Exception):
                      log.error(f"Error stopping DIF '{difs_names[i]}': {res}", exc_info=res)
        self._difs.clear()

        log.info("Stopping all Network Interfaces...")
        async with self._interfaces_lock:
            tasks = [asyncio.create_task(inf.stop(), name=f"stop_if_{inf_id}")
                     for inf_id, inf in self._interfaces.items() if hasattr(inf, 'stop')]
            results = await asyncio.gather(*tasks, return_exceptions=True)
             # Log exceptions during Interface stop
            interfaces_ids = [inf_id for inf_id, inf in self._interfaces.items() if hasattr(inf, 'stop')]
            for i, res in enumerate(results):
                 if isinstance(res, Exception):
                      log.error(f"Error stopping Interface '{interfaces_ids[i]}': {res}", exc_info=res)
        self._interfaces.clear()

        log.info("LayerManager stopped.")

    def __repr__(self) -> str:
        return (f"LayerManager(DIFs:{list(self._difs.keys())}, "
                f"IPCPs:{list(self._ipcps.keys())}, Interfaces:{list(self._interfaces.keys())})")