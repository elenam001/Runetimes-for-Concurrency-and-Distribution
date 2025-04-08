import asyncio
import logging
from typing import Dict, Optional, Any, TYPE_CHECKING

# Use forward references or TYPE_CHECKING for type hints
if TYPE_CHECKING:
    from ..core.dif import DIF
    from ..core.ipcp import IPCProcess, ApplicationHandle, IPCProcessState #attenzione ProcessState
    from ..core.naming import ApplicationName, DIFName
    from .policy import PolicyManager
    # Assume an interface abstraction for underlying network access
    from ..interfaces.network_interface import NetworkInterface

log = logging.getLogger(__name__)

class LayerManager:
    """
    Manages the creation, configuration, and lifecycle of DIFs and IPCPs
    in a RINA stack. Handles the recursive layering of DIFs.
    """

    def __init__(self):
        self._difs: Dict['DIFName', 'DIF'] = {}
        # Store all IPCPs managed by this layer manager for easy lookup
        self._ipcps: Dict['ApplicationName', 'IPCProcess'] = {}
        # Store network interfaces (e.g., DIF 0 implementations)
        self._interfaces: Dict[str, 'NetworkInterface'] = {}
        self._difs_lock = asyncio.Lock()
        self._ipcps_lock = asyncio.Lock()
        self._interfaces_lock = asyncio.Lock()

        log.info("LayerManager initialized.")

    async def create_dif(self,
                         dif_name: 'DIFName',
                         config: Optional[Dict[str, Any]] = None,
                         policy_manager: Optional['PolicyManager'] = None) -> Optional['DIF']:
        """Creates a new DIF instance and registers it."""
        from ..core.dif import DIF # Import locally to prevent circularity at import time

        async with self._difs_lock:
            if dif_name in self._difs:
                log.warning(f"DIF '{dif_name}' already exists.")
                return self._difs[dif_name]

            log.info(f"Creating DIF '{dif_name}'...")
            new_dif = DIF(
                dif_name=dif_name,
                layer_manager=self,
                policy_manager=policy_manager,
                config=config
            )
            self._difs[dif_name] = new_dif
            log.info(f"DIF '{dif_name}' created.")
            return new_dif

    async def get_dif(self, dif_name: 'DIFName') -> Optional['DIF']:
        """Retrieves a DIF instance by name."""
        async with self._difs_lock:
            return self._difs.get(dif_name)

    async def create_ipcp(self,
                          app_name: 'ApplicationName',
                          target_dif_name: 'DIFName',
                          app_handle: Optional['ApplicationHandle'] = None) -> Optional['IPCProcess']:
        """
        Creates an IPC Process instance intended to join the target DIF.
        Does not automatically enroll it.
        """
        from ..core.ipcp import IPCProcess # Local import

        parent_dif = await self.get_dif(target_dif_name)
        if not parent_dif:
            log.error(f"Cannot create IPCP '{app_name}', target DIF '{target_dif_name}' not found.")
            return None

        async with self._ipcps_lock:
            if app_name in self._ipcps:
                log.warning(f"IPCP with Application Name '{app_name}' already exists.")
                # Decide on behavior: return existing or error? Let's return existing.
                return self._ipcps[app_name]

            log.info(f"Creating IPCP for Application '{app_name}' targeting DIF '{target_dif_name}'...")
            new_ipcp = IPCProcess(
                app_name=app_name,
                parent_dif=parent_dif,
                layer_manager=self,
                app_handle=app_handle
            )
            self._ipcps[app_name] = new_ipcp
            log.info(f"IPCP for '{app_name}' created.")
            # Note: IPCP still needs underlying IPCPs assigned via stack_dif_over
            # and then needs to be started and enrolled.
            return new_ipcp

    async def get_ipcp(self, app_name: 'ApplicationName') -> Optional['IPCProcess']:
        """Retrieves an IPCP instance by Application Name."""
        async with self._ipcps_lock:
            return self._ipcps.get(app_name)

    async def add_network_interface(self, interface_id: str, interface: 'NetworkInterface'):
         """Registers a low-level network interface (e.g., UDP socket wrapper)."""
         async with self._interfaces_lock:
              if interface_id in self._interfaces:
                   log.warning(f"Network interface '{interface_id}' already registered.")
                   return
              self._interfaces[interface_id] = interface
              log.info(f"Network interface '{interface_id}' added.")

    async def stack_dif_over(self, upper_dif_name: 'DIFName', lower_layer_id: str):
        """
        Configures the layering: makes upper DIF use the lower layer for transport.
        This is a simplified version. Real RINA involves creating management IPCPs
        in the lower layer to support the upper DIF.

        Args:
            upper_dif_name: Name of the DIF being supported.
            lower_layer_id: Name of the supporting DIF OR ID of a NetworkInterface.
        """
        log.info(f"Attempting to stack DIF '{upper_dif_name}' over '{lower_layer_id}'...")

        upper_dif = await self.get_dif(upper_dif_name)
        if not upper_dif:
            log.error(f"Cannot stack, upper DIF '{upper_dif_name}' not found.")
            return

        # Check if lower layer is another DIF or a base network interface
        lower_dif = await self.get_dif(DIFName(lower_layer_id))
        lower_interface = None
        async with self._interfaces_lock:
             if not lower_dif:
                  lower_interface = self._interfaces.get(lower_layer_id)

        if not lower_dif and not lower_interface:
             log.error(f"Cannot stack, lower layer '{lower_layer_id}' (DIF or Interface) not found.")
             return

        # --- Simplified Stacking Logic ---
        # Assign underlying transport to IPCPs created *for* the upper DIF.
        # This assumes IPCPs are created *before* stacking is defined.
        # A better approach might dynamically assign transport when an IPCP enrolls.

        async with self._ipcps_lock:
             ipcps_in_upper_dif = [ipcp for ipcp in self._ipcps.values() if ipcp.parent_dif == upper_dif]

             if not ipcps_in_upper_dif:
                  log.warning(f"No IPCPs found targeting upper DIF '{upper_dif_name}' yet. Underlying transport cannot be assigned now.")
                  # Store pending relationship? Or require IPCPs later?

             for ipcp in ipcps_in_upper_dif:
                  if lower_dif:
                       # Find or create an IPCP in the lower DIF to serve this upper IPCP.
                       # This is where complex mapping/management happens in real RINA.
                       # Simplification: Assume a *single* designated IPCP in the lower DIF handles all traffic for this node.
                       # We need a way to identify that 'node representative' IPCP.
                       # Placeholder: Assume there's one named '<lower_dif_name>-node'
                       node_ipcp_name = ApplicationName(f"{lower_layer_id}-node")
                       underlying_ipcp = await self.get_ipcp(node_ipcp_name)
                       if not underlying_ipcp:
                            log.error(f"Cannot stack '{ipcp.app_name}' over DIF '{lower_layer_id}'. "
                                      f"Node representative IPCP '{node_ipcp_name}' not found in lower DIF.")
                            continue # Skip this IPCP

                       ipcp.add_underlying_ipcp(DIFName(lower_layer_id), underlying_ipcp)
                       log.info(f"IPCP '{ipcp.app_name}' in '{upper_dif_name}' will use IPCP '{underlying_ipcp.app_name}' in lower DIF '{lower_layer_id}'.")

                  elif lower_interface:
                       # The lower layer is a base network interface.
                       # The IPCP needs to be directly associated with this interface.
                       # This requires the IPCP to know how to interact with the NetworkInterface API.
                       # Simplification: Assume the IPCP can directly use the interface handle.
                       ipcp.assign_network_interface(lower_layer_id, lower_interface) # Requires adding this method to IPCP
                       log.info(f"IPCP '{ipcp.app_name}' in '{upper_dif_name}' will use Network Interface '{lower_layer_id}'.")

        # TODO: Store the stacking relationship explicitly (e.g., upper_dif.runs_over = [lower_layer_id])

    async def enroll_ipcp(self, app_name: 'ApplicationName') -> bool:
        """Finds an IPCP and initiates its enrollment process."""
        ipcp = await self.get_ipcp(app_name)
        if not ipcp:
            log.error(f"Cannot enroll, IPCP '{app_name}' not found.")
            return False
        if ipcp.state != IPCProcessState.UNENROLLED: # Assuming IPCProcessState is imported
             log.warning(f"IPCP '{app_name}' is not in UNENROLLED state ({ipcp.state}). Cannot initiate enrollment.")
             return False

        # Ensure underlying transport is assigned *before* enrolling
        # if not ipcp._underlying_ipcps and not ipcp.network_interface: # Check if transport assigned
        #      log.error(f"Cannot enroll IPCP '{app_name}', no underlying transport (DIF or Interface) assigned via stack_dif_over.")
        #      return False

        return await ipcp.enroll()

    async def start_all(self):
        """Starts all managed DIFs and IPCPs."""
        log.info("Starting all DIFs...")
        async with self._difs_lock:
            for dif_name, dif_obj in self._difs.items():
                log.info(f"Starting DIF '{dif_name}'...")
                await dif_obj.start()

        log.info("Starting all IPCPs...")
        async with self._ipcps_lock:
            for app_name, ipcp_obj in self._ipcps.items():
                 # IPCPs should be started *before* enrollment is attempted
                 log.info(f"Starting IPCP '{app_name}'...")
                 await ipcp_obj.start()

        # Note: Enrollment is a separate step, typically initiated after starting.

    async def stop_all(self):
        """Stops all managed DIFs and IPCPs gracefully."""
        log.info("Stopping all IPCPs...")
        async with self._ipcps_lock:
            # Create list of tasks to stop IPCPs concurrently
            stop_tasks = [ipcp_obj.stop() for ipcp_obj in self._ipcps.values()]
            await asyncio.gather(*stop_tasks) # Wait for all IPCPs to stop
        self._ipcps.clear() # Clear after stopping

        log.info("Stopping all DIFs...")
        async with self._difs_lock:
            stop_tasks = [dif_obj.stop() for dif_obj in self._difs.values()]
            await asyncio.gather(*stop_tasks) # Wait for all DIFs to stop
        self._difs.clear() # Clear after stopping

        log.info("LayerManager stopped.")

    def __repr__(self) -> str:
        return (f"LayerManager(DIFs:{list(self._difs.keys())}, "
                f"IPCPs:{list(self._ipcps.keys())}, Interfaces:{list(self._interfaces.keys())})")