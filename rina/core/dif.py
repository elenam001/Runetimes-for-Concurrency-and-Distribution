# rina/core/dif.py

import asyncio
import logging
import json # For parsing management PDU payloads
import itertools # For port allocation
import random
from typing import Dict, Optional, Any, TYPE_CHECKING, Union

# Core RINA imports
from .pdu import PDU, PduType, PduFlags # Import PDU definitions
from .naming import ApplicationName, PortId, DIFName # Import naming types

# Import necessary types for runtime checks if not just in TYPE_CHECKING
from .ipcp import IPCProcess
from ..interfaces.network_interface import NetworkInterface, UnderlyingAddress

# Setup logger
log = logging.getLogger(__name__)

# Avoid circular imports
if TYPE_CHECKING:
    # These are already imported above for runtime, but good practice
    # from .ipcp import IPCProcess
    # from ..interfaces.network_interface import NetworkInterface, UnderlyingAddress
    from ..management.layer_manager import LayerManager
    from ..management.policy import PolicyManager


# --- Constants ---
# Define dummy addresses for DIF management services (matching IPCProcess)
# These need resolution via LayerManager to actual underlying addresses
DIF_ENROLLMENT_ADDRESS = ApplicationName("dif_enrollment_service")
DIF_FLOW_ALLOCATOR_ADDRESS = ApplicationName("dif_flow_allocator_service")

# Add a new PDU type conceptually needed for notifying target of flow setup
# Ensure this is added to pdu.py's PduType enum as well!
# PduType.MGMT_FLOW_SETUP_NOTIFY = 15 # Example ID

class DIF:
    """
    Represents a Distributed IPC Facility (DIF).

    Manages IPCPs, policies, naming, and processes management PDUs
    (Enrollment, Flow Allocation) received via underlying transport.
    Sends management responses and notifications.
    """

    def __init__(self,
                 dif_name: DIFName,
                 layer_manager: 'LayerManager',
                 policy_manager: Optional['PolicyManager'] = None,
                 config: Optional[Dict[str, Any]] = None):
        """Initializes a Distributed IPC Facility."""
        self.dif_name: DIFName = dif_name
        self.layer_manager: 'LayerManager' = layer_manager
        self.policy_manager: Optional['PolicyManager'] = policy_manager
        self.config: Dict[str, Any] = config if config else {}

        # Registered IPCPs (Key: ApplicationName as str)
        self._ipc_processes: Dict[str, 'IPCProcess'] = {}
        self._ipcps_lock = asyncio.Lock()

        # Simple Port ID allocation
        self._port_allocator = itertools.count(start=10000)
        self._allocated_ports = set()
        self._port_lock = asyncio.Lock()

        # DIF's own transport for management PDUs (set by LayerManager)
        self.management_transport: Optional[Union['IPCProcess', 'NetworkInterface']] = None
        self.management_address: Optional[ApplicationName] = None # This DIF's logical mgmt address

        log.info(f"DIF '{self.dif_name}' initialized.")

    # --- Lifecycle ---
    async def start(self):
        """Starts the DIF's operational tasks."""
        log.info(f"Starting DIF '{self.dif_name}'...")
        if not self.management_transport:
             log.warning(f"DIF '{self.dif_name}' starting without configured management transport. Cannot receive requests.")
        # TODO: Start internal DIF management tasks/listeners if needed.
        await asyncio.sleep(0.01)
        log.info(f"DIF '{self.dif_name}' started.")

    async def stop(self):
        """Stops the DIF's operational tasks and cleans up resources."""
        log.info(f"Stopping DIF '{self.dif_name}'...")
        # TODO: Stop management tasks/listeners
        async with self._ipcps_lock:
            self._ipc_processes.clear()
        async with self._port_lock:
            self._allocated_ports.clear()
        await asyncio.sleep(0.01)
        log.info(f"DIF '{self.dif_name}' stopped.")

    # --- IPCP Registration (Local cache) ---
    async def register_ipcp(self, app_name: ApplicationName, ipcp: 'IPCProcess'):
        """Registers an IPCP locally after successful enrollment confirmation."""
        async with self._ipcps_lock:
            app_name_str = str(app_name)
            if app_name_str in self._ipc_processes:
                log.warning(f"DIF '{self.dif_name}': App name '{app_name_str}' already registered. Overwriting.")
            self._ipc_processes[app_name_str] = ipcp
            log.info(f"DIF '{self.dif_name}': Locally registered IPCP for app '{app_name_str}'.")
            return True

    async def unregister_ipcp(self, app_name: ApplicationName):
        """Unregisters an IPCP locally."""
        async with self._ipcps_lock:
            app_name_str = str(app_name)
            removed_ipcp = self._ipc_processes.pop(app_name_str, None)
            if removed_ipcp:
                log.info(f"DIF '{self.dif_name}': Locally unregistered IPCP for app '{app_name_str}'.")
            return removed_ipcp

    async def resolve_name(self, app_name: ApplicationName) -> Optional['IPCProcess']:
        """Finds a *locally known* (enrolled) IPCP instance by application name."""
        async with self._ipcps_lock:
            # TODO: Implement distributed directory lookup if DIF spans multiple nodes.
            return self._ipc_processes.get(str(app_name))

    # --- Port Allocation ---
    async def _allocate_port_id(self) -> Optional[PortId]:
         """Allocates a unique PortId within this DIF."""
         async with self._port_lock:
              attempts = 0
              max_attempts = 1000 # Avoid infinite loop in unlikely scenario
              while attempts < max_attempts:
                   port = PortId(next(self._port_allocator))
                   if port not in self._allocated_ports:
                        self._allocated_ports.add(port)
                        log.debug(f"DIF '{self.dif_name}': Allocated PortId {port}")
                        return port
                   attempts += 1
              log.error(f"DIF '{self.dif_name}': Failed to allocate unique PortId after {max_attempts} attempts.")
              return None

    async def _release_port_id(self, port_id: PortId):
         """Releases a previously allocated PortId."""
         async with self._port_lock:
              self._allocated_ports.discard(port_id) # Use discard to avoid error if not present
              log.debug(f"DIF '{self.dif_name}': Released PortId {port_id}")

    # --- Incoming Management PDU Handling ---
    async def receive_management_pdu(self, pdu: PDU, source_underlying_address: UnderlyingAddress):
        """
        Entry point for management PDUs routed to this DIF.
        Parses the PDU and dispatches to the appropriate handler.
        Called by the DIF's management_transport (IPCP proxy or direct handler).
        """
        log.debug(f"DIF '{self.dif_name}': Received Management PDU: {pdu} from {source_underlying_address}")

        try:
            # Extract target DIF name from the payload (added during enrollment)
            payload_data = json.loads(pdu.payload.decode('utf-8'))
            target_dif_name = DIFName(payload_data.get("target_dif"))
            if target_dif_name and target_dif_name != self.dif_name:
                # Forward to the target DIF
                target_dif = await self.layer_manager.get_dif(target_dif_name)
                if target_dif:
                    log.info(f"DIF '{self.dif_name}': Forwarding PDU to DIF '{target_dif_name}'")
                    await target_dif.receive_management_pdu(pdu, source_underlying_address)
                    return
        except Exception as e:
            log.error(f"DIF '{self.dif_name}': Failed to route PDU: {e}")

        # Default to processing locally if target DIF matches or extraction fails
        await self._process_management_pdu_locally(pdu, source_underlying_address)

    async def _process_management_pdu_locally(self, pdu: PDU, source_underlying_address: UnderlyingAddress):
        """Process PDUs intended for this DIF."""
        # Existing handler logic (dispatch to _handle_enrollment_request_pdu, etc.)
        handler_map = {
            PduType.MGMT_ENROLL_REQ: self._handle_enrollment_request_pdu,
            PduType.MGMT_FLOW_ALLOC_REQ: self._handle_flow_request_pdu,
        }
        handler = handler_map.get(pdu.pdu_type)
        if handler:
            asyncio.create_task(handler(pdu, source_underlying_address))
        else:
            log.warning(f"DIF '{self.dif_name}': Unhandled PDU type {pdu.pdu_type.name}")

    # --- PDU-based Handlers ---
    async def _handle_enrollment_request_pdu(self, pdu: PDU, source_underlying_address: UnderlyingAddress):
        """Processes an MGMT_ENROLL_REQ PDU and sends a response PDU."""
        response_payload = {}
        status = "failure"
        reason = "Internal processing error"
        tid = None # Initialize tid

        try:
            request_data = json.loads(pdu.payload.decode('utf-8'))
            app_name_req = ApplicationName(request_data["app_name"]) # Use [] for required fields
            tid = request_data["tid"]

            log.info(f"DIF '{self.dif_name}': Processing enrollment request PDU for '{app_name_req}' (TID: {tid}) from {source_underlying_address}")

            # 1. Policy Check
            allowed = True # Placeholder
            if self.policy_manager:
                # allowed = await self.policy_manager.check_enrollment(request_data)
                 pass

            if not allowed:
                reason = "Policy denied"
                log.warning(f"DIF '{self.dif_name}': Enrollment denied by policy for '{app_name_req}' (TID: {tid}).")
            else:
                # 2. Resource Allocation (Placeholder)
                # 3. Assign Address (Placeholder)
                assigned_address = f"{app_name_req}_addr_{random.randint(100,999)}"
                # 4. Success
                status = "success"
                reason = "Enrollment successful"
                log.info(f"DIF '{self.dif_name}': Enrollment approved for '{app_name_req}' (TID: {tid}). Assigned addr: {assigned_address}")
                response_payload = {
                    "status": status, "reason": reason, "tid": tid, # Include TID always
                    "assigned_dif_name": self.dif_name,
                    "assigned_address": assigned_address,
                }
        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
            reason = f"Invalid enrollment request PDU payload: {e}"
            log.error(f"DIF '{self.dif_name}': {reason}. PDU: {pdu}", exc_info=True)
            # Try to extract TID even if parsing failed partially
            try: tid = json.loads(pdu.payload.decode('utf-8')).get("tid")
            except: pass
        except Exception as e:
             log.error(f"DIF '{self.dif_name}': Unexpected error handling enrollment PDU (TID: {tid}): {e}", exc_info=True)
             # Try to extract TID
             try: tid = json.loads(pdu.payload.decode('utf-8')).get("tid")
             except: pass

        # Construct and send response PDU
        if status != "success":
             response_payload = {"status": status, "reason": reason}
             if tid is not None: response_payload["tid"] = tid # Add TID if available

        response_pdu = PDU(
            pdu_type=PduType.MGMT_ENROLL_RESP,
            dest_port_id=pdu.src_port_id, # Target original source port (if used)
            src_port_id=PortId(0), # Source is DIF management
            payload=json.dumps(response_payload).encode('utf-8')
        )
        await self._send_management_pdu(response_pdu, destination_underlying_address=source_underlying_address)

    async def _handle_flow_request_pdu(self, pdu: PDU, source_underlying_address: UnderlyingAddress):
        """Processes an MGMT_FLOW_ALLOC_REQ PDU, sends response, and notifies target."""
        response_payload = {}
        status = "failure"
        reason = "Internal processing error"
        flow_info = {}
        tid = None
        source_ipcp = None # Keep track for notification step
        target_ipcp = None

        try:
            request_data = json.loads(pdu.payload.decode('utf-8'))
            source_app_name = ApplicationName(request_data["source_app_name"])
            target_app_name = ApplicationName(request_data["target_app_name"])
            qos_params = request_data.get("qos_params", {})
            tid = request_data["tid"]

            log.info(f"DIF '{self.dif_name}': Processing flow req PDU from '{source_app_name}' to '{target_app_name}' (TID: {tid})")

            # 1. Resolve Names (check if IPCPs are enrolled locally)
            source_ipcp = await self.resolve_name(source_app_name)
            target_ipcp = await self.resolve_name(target_app_name)

            if not source_ipcp or not target_ipcp:
                reason = "Source or target application not found/enrolled in this DIF"
                log.warning(f"DIF '{self.dif_name}': Flow request failed (TID: {tid}). {reason}")
            else:
                # 2. Policy Check
                allowed = True # Placeholder
                if self.policy_manager:
                     # allowed = await self.policy_manager.check_flow_request(source_ipcp, target_ipcp, qos_params)
                     pass

                if not allowed:
                    reason = "Policy denied"
                    log.warning(f"DIF '{self.dif_name}': Flow request denied by policy (TID: {tid}).")
                else:
                    # 3. Resource/Negotiation (Placeholder)
                    negotiated_qos = qos_params

                    # 4. Allocate Port IDs
                    source_port_id = await self._allocate_port_id()
                    dest_port_id = await self._allocate_port_id()

                    if not source_port_id or not dest_port_id:
                         reason = "Failed to allocate port IDs"
                         log.error(f"DIF '{self.dif_name}': {reason} (TID: {tid})")
                         if source_port_id: await self._release_port_id(source_port_id)
                         if dest_port_id: await self._release_port_id(dest_port_id)
                    else:
                         # 5. Success - Prepare response info for source
                         status = "success"
                         reason = "Flow allocated successfully"
                         flow_info = {
                             "source_port_id": source_port_id,
                             "destination_port_id": dest_port_id, # Port target should use
                             "target_app_name": str(target_app_name), # Echo back for source confirmation
                             "negotiated_qos": negotiated_qos
                         }
                         log.info(f"DIF '{self.dif_name}': Flow allocated for TID {tid}. Src({source_app_name}:{source_port_id}) <-> Dst({target_app_name}:{dest_port_id})")

        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
            reason = f"Invalid flow request PDU payload: {e}"
            log.error(f"DIF '{self.dif_name}': {reason}. PDU: {pdu}", exc_info=True)
            try: tid = json.loads(pdu.payload.decode('utf-8')).get("tid")
            except: pass
        except Exception as e:
             log.error(f"DIF '{self.dif_name}': Unexpected error handling flow request PDU (TID: {tid}): {e}", exc_info=True)
             try: tid = json.loads(pdu.payload.decode('utf-8')).get("tid")
             except: pass

        # Construct response payload for source IPCP
        if status == "success":
             response_payload = {"status": status, "reason": reason, "flow_info": flow_info}
        else:
             response_payload = {"status": status, "reason": reason}
        if tid is not None: response_payload["tid"] = tid

        # Send response PDU back to the *source* IPCP
        response_pdu = PDU(
            pdu_type=PduType.MGMT_FLOW_ALLOC_RESP,
            dest_port_id=pdu.src_port_id, # Target original source port (if used)
            src_port_id=PortId(0),
            payload=json.dumps(response_payload).encode('utf-8')
        )
        await self._send_management_pdu(response_pdu, destination_underlying_address=source_underlying_address)

        # --- Notify Target IPCP (if allocation was successful) ---
        if status == "success" and target_ipcp:
            # Ensure PduType.MGMT_FLOW_SETUP_NOTIFY exists in pdu.py enum
            if hasattr(PduType, 'MGMT_FLOW_SETUP_NOTIFY'):
                log.info(f"DIF '{self.dif_name}': Notifying target IPCP '{target_app_name}' about incoming flow setup.")
                notification_payload = {
                    "source_app_name": str(source_app_name), # Let target know who initiated
                    "source_port_id": source_port_id, # Port source is using
                    "destination_port_id": dest_port_id, # Port target should listen on
                    "negotiated_qos": negotiated_qos
                }
                notification_pdu = PDU(
                    pdu_type=PduType.MGMT_FLOW_SETUP_NOTIFY,
                    dest_port_id=PortId(0), # Target is the IPCP itself
                    src_port_id=PortId(0), # Source is DIF management
                    payload=json.dumps(notification_payload).encode('utf-8')
                )
                # Resolve target IPCP's underlying address
                target_underlying_address = await self.layer_manager.resolve_app_to_underlying_address(target_app_name)
                if target_underlying_address:
                    await self._send_management_pdu(notification_pdu, destination_underlying_address=target_underlying_address)
                else:
                    log.error(f"DIF '{self.dif_name}': Failed to resolve underlying address for target '{target_app_name}' to send flow notification.")
                    # TODO: How to handle this failure? Flow might be half-open. Deallocate?
                    await self._release_port_id(source_port_id)
                    await self._release_port_id(dest_port_id)
            else:
                 log.warning(f"DIF '{self.dif_name}': PduType.MGMT_FLOW_SETUP_NOTIFY not defined. Cannot notify target IPCP.")

    # --- Sending Management PDUs (Responses/Notifications) ---
    async def _send_management_pdu(self, pdu: PDU, destination_underlying_address: UnderlyingAddress):
        """Sends a management PDU (response or notification) using the DIF's transport."""
        log.debug(f"DIF '{self.dif_name}': Sending management PDU: {pdu} to {destination_underlying_address}")

        if not self.management_transport:
            log.error(f"DIF '{self.dif_name}': Cannot send PDU, no management transport configured.")
            return False

        # PDU object is passed directly now, serialization happens lower down if needed
        # or within the transport's send method. Let's assume IPCP transport takes PDU.

        try:
            if isinstance(self.management_transport, IPCProcess):
                log.debug(f"DIF '{self.dif_name}': Relaying PDU via Management IPCP '{self.management_transport.app_name}'")
                success = await self.management_transport._send_on_underlying(
                    pdu, destination_underlying_address # Pass final dest addr
                )
            elif isinstance(self.management_transport, NetworkInterface):
                 # Send directly using the Network Interface
                 pdu_bytes = pdu.to_bytes()
                 if not pdu_bytes:
                      log.error(f"DIF '{self.dif_name}': Failed to serialize PDU {pdu} for interface send.")
                      return False
                 success = await self.management_transport.send(pdu_bytes, destination_underlying_address)
            else:
                 log.error(f"DIF '{self.dif_name}': Unknown management transport type: {type(self.management_transport)}")
                 success = False

            if not success:
                 log.error(f"DIF '{self.dif_name}': Failed sending PDU via management transport to {destination_underlying_address}.")
            return success

        except Exception as e:
            log.error(f"DIF '{self.dif_name}': Exception sending PDU to {destination_underlying_address}: {e}", exc_info=True)
            return False


    def __repr__(self) -> str:
        num_ipcps = len(self._ipc_processes)
        mgmt_transport_info = type(self.management_transport).__name__ if self.management_transport else "None"
        return f"DIF(name='{self.dif_name}', ipcps={num_ipcps}, mgmt_transport={mgmt_transport_info})"