# rina/core/ipcp.py

import asyncio
import logging
import json # Using json for simple management PDU payload serialization
import random # For generating transaction IDs
from enum import Enum, auto
from typing import Dict, List, Optional, Any, TYPE_CHECKING, Callable, Union

# Core RINA imports
from .naming import ApplicationName, PortId, DIFName
# Import specific PDU types and base class
from .pdu import PDU, PduType, PduFlags
from ..interfaces.network_interface import NetworkInterface, UnderlyingAddress


#from rina.interfaces.network_interface import NetworkInterface, UnderlyingAddress
try:
    from .flow import Flow
except ImportError:
     from rina.core.flow import Flow # Adjust path

# Setup logger for this module
log = logging.getLogger(__name__)

# Avoid circular imports for type hints
if TYPE_CHECKING:
    from .dif import DIF
    # from .flow import Flow # Already imported above
    from ..management.layer_manager import LayerManager
    # from ..interfaces.network_interface import NetworkInterface, UnderlyingAddress # Already imported
    # Define a type alias for the application callback/handle
    ApplicationHandle = Callable[[PortId, bytes], asyncio.Future]


# --- Constants ---
# Define logical names for DIF management services
DIF_ENROLLMENT_ADDRESS = ApplicationName("dif_enrollment_service")
DIF_FLOW_ALLOCATOR_ADDRESS = ApplicationName("dif_flow_allocator_service")

REQUEST_TIMEOUT = 5.0 # Timeout for waiting for responses (in seconds)

class IPCProcessState(Enum):
    """Represents the enrollment state of an IPC Process within a DIF."""
    NULL = auto()
    UNENROLLED = auto()
    ENROLLING = auto()
    ENROLLED = auto()
    FAILED_ENROLLMENT = auto()
    UNENROLLING = auto()


class IPCProcess:
    """
    Represents an IPC Process (IPCP).
    Handles enrollment, flow management, and data transfer using PDUs
    over underlying transport (other IPCPs or Network Interfaces).
    Acts as handler for incoming packets from Network Interface if designated.
    """

    def __init__(self,
                 app_name: ApplicationName,
                 parent_dif: 'DIF',
                 layer_manager: 'LayerManager',
                 app_handle: Optional['ApplicationHandle'] = None):
        self.app_name: ApplicationName = app_name
        self.parent_dif: 'DIF' = parent_dif
        self.layer_manager: 'LayerManager' = layer_manager
        self.app_handle: Optional['ApplicationHandle'] = app_handle

        self.state: IPCProcessState = IPCProcessState.NULL
        self.local_address: Optional[str] = None # DIF-specific assigned address

        # Flow management
        self._flows: Dict[PortId, 'Flow'] = {}
        self._flows_lock = asyncio.Lock()

        # Underlying transport
        self._underlying_ipcps: Dict[DIFName, 'IPCProcess'] = {}
        self.network_interface_id: Optional[str] = None
        self.network_interface: Optional['NetworkInterface'] = None

        # Tracking pending requests (Transaction ID -> Future)
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._request_lock = asyncio.Lock() # Lock for accessing pending requests

        log.info(f"IPCP for '{self.app_name}' created (State: {self.state}). Belongs to DIF '{self.parent_dif.dif_name}'.")

    # --- Lifecycle Methods ---
    async def start(self):
        """Starts the IPCP's background tasks, if any."""
        log.info(f"IPCP '{self.app_name}': Starting...")
        self.state = IPCProcessState.UNENROLLED # Ready to enroll
        log.info(f"IPCP '{self.app_name}': Started (State: {self.state}).")

    async def stop(self):
        """Stops the IPCP, cleans up resources, and unenrolls."""
        log.info(f"IPCP '{self.app_name}': Stopping...")
        if self.state == IPCProcessState.ENROLLED:
            await self.unenroll() # Attempt graceful unenrollment

        # Cancel pending requests
        async with self._request_lock:
            pending_futures = list(self._pending_requests.values()) # Get futures before clearing
            self._pending_requests.clear()
            for future in pending_futures:
                 if not future.done():
                      try:
                          future.set_exception(asyncio.CancelledError(f"IPCP {self.app_name} stopping"))
                      except asyncio.InvalidStateError: pass # Future might already be cancelled

        # Terminate flows
        async with self._flows_lock:
            flows_to_close = list(self._flows.values())
            self._flows.clear() # Clear dict first
            flow_close_tasks = [flow.close() for flow in flows_to_close]
            # Use gather to run closes concurrently, ignore exceptions during shutdown
            await asyncio.gather(*flow_close_tasks, return_exceptions=True)


        self.state = IPCProcessState.NULL
        log.info(f"IPCP '{self.app_name}': Stopped (State: {self.state}).")

    # --- Underlying Transport Configuration ---
    def add_underlying_ipcp(self, dif_name: DIFName, ipcp: 'IPCProcess'):
        """Registers an IPCP in an underlying DIF used for transport."""
        if self.network_interface:
             log.warning(f"IPCP '{self.app_name}': Adding underlying IPCP while Network Interface is assigned. Check config.")
        log.info(f"IPCP '{self.app_name}': Adding underlying IPCP '{ipcp.app_name}' in DIF '{dif_name}'.")
        self._underlying_ipcps[dif_name] = ipcp

    def assign_network_interface(self, interface_id: str, interface: NetworkInterface):
        """Assigns a network interface for transport (typically for DIF 0 IPCPs)."""
        if self._underlying_ipcps:
            log.warning(f"IPCP '{self.app_name}': Assigning network interface while underlying IPCPs exist. Check configuration.")
        self.network_interface_id = interface_id
        self.network_interface = interface
        log.info(f"IPCP '{self.app_name}': Assigned Network Interface '{interface_id}'.")

    # --- Enrollment ---
    async def enroll(self) -> bool:
        """
        Initiates enrollment by sending an MGMT_ENROLL_REQ PDU. Waits for response.
        Returns True on success, False on failure or timeout.
        """
        if self.state != IPCProcessState.UNENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot enroll, state is {self.state}.")
            return False
        if not self._underlying_ipcps and not self.network_interface:
             log.error(f"IPCP '{self.app_name}': Cannot enroll, no underlying transport configured.")
             return False

        log.info(f"IPCP '{self.app_name}': Attempting enrollment into DIF '{self.parent_dif.dif_name}' via PDU...")
        self.state = IPCProcessState.ENROLLING

        tid = random.randint(1, 2**31)
        request_payload = json.dumps({
            "app_name": str(self.app_name), # Ensure app_name is serializable string
            "tid": tid,
            "target_dif": str(self.parent_dif.dif_name)
        }).encode('utf-8')
        source_address = await self.layer_manager.resolve_app_to_underlying_address(self.app_name, self)
        enroll_request_pdu = PDU(
            pdu_type=PduType.MGMT_ENROLL_REQ, # Use specific type
            dest_port_id=PortId(0), # Port ID likely irrelevant for DIF service
            src_port_id=PortId(0),
            payload=request_payload,
            source_address=source_address  # Add this field if not already present
        )
        target_service_address = DIF_ENROLLMENT_ADDRESS # Logical service name

        enroll_future = asyncio.Future()
        log.debug(f"IPCP '{self.app_name}': Created enroll_future (ID: {id(enroll_future)}) for TID {tid}")
        async with self._request_lock:
            self._pending_requests[tid] = enroll_future

        result = False # Default to failure
        try:
            # Send PDU targeting the logical service address
            sent = await self._send_on_underlying(enroll_request_pdu, target_service_address)
            if not sent:
                # Set future to False explicitly before raising
                if not enroll_future.done(): enroll_future.set_result(False)
                raise ConnectionError("Failed to send enrollment request PDU.")

            log.debug(f"IPCP '{self.app_name}': Enrollment request sent (TID: {tid}), waiting {REQUEST_TIMEOUT}s for response...")
            result = await asyncio.wait_for(enroll_future, timeout=REQUEST_TIMEOUT)
            # Result is now whatever the handler set (True/False)
            log.info(f"IPCP '{self.app_name}': Enrollment result received (TID: {tid}): {result}")

        except asyncio.TimeoutError:
            log.error(f"IPCP '{self.app_name}': Enrollment request (TID: {tid}) timed out.")
            if self.state == IPCProcessState.ENROLLING: # Check state before changing
                 self.state = IPCProcessState.FAILED_ENROLLMENT
            result = False # Ensure failure return on timeout
        except Exception as e:
            log.error(f"IPCP '{self.app_name}': Exception during enrollment PDU handling (TID: {tid}): {e}", exc_info=True)
            if self.state == IPCProcessState.ENROLLING:
                 self.state = IPCProcessState.FAILED_ENROLLMENT
            result = False # Ensure failure return on other exceptions
        finally:
            # --- Re-add finally block to ensure cleanup of pending request ---
            async with self._request_lock:
                removed_future = self._pending_requests.pop(tid, None)
                # Optional: Check if future was already resolved by handler vs timeout/exception
                if removed_future and not removed_future.done() and not result:
                     log.warning(f"IPCP '{self.app_name}': Removed pending enrollment future (TID: {tid}) that failed without being resolved.")
                     # Potentially set exception on future if it wasn't done? Depends on desired behavior.
                     # removed_future.set_exception(ConnectionAbortedError("Request failed and cleaned up"))
            # --- End Re-add ---
        return result

    # --- Make handler async ---
    async def _handle_enrollment_response(self, pdu: PDU):
        """Processes an MGMT_ENROLL_RESP PDU."""
        tid = None # Initialize tid for use in exception blocks
        enroll_future = None # Initialize future
        try:
            response_data = json.loads(pdu.payload.decode('utf-8'))
            tid = response_data.get("tid")
            status = response_data.get("status")
            reason = response_data.get("reason")
            if tid is None: raise ValueError("Missing tid in response payload")

            # Find the future early, but check if done before setting result
            async with self._request_lock:
                enroll_future = self._pending_requests.get(tid)
                if enroll_future: log.debug(f"IPCP '{self.app_name}': Found pending enroll_future (ID: {id(enroll_future)}) for TID {tid}. Done={enroll_future.done()}")
                else: log.warning(f"IPCP '{self.app_name}': No pending future found for TID {tid} in handler.")

            if not enroll_future or enroll_future.done():
                 log.warning(f"IPCP '{self.app_name}': Ignoring enrollment response for TID {tid} - no pending/valid future found.")
                 return # Exit if no valid future to set

            log.debug(f"IPCP '{self.app_name}': Handling enrollment response for TID {tid}. Status: {status}")

            if status == "success":
                if self.state == IPCProcessState.ENROLLING:
                    self.state = IPCProcessState.ENROLLED
                    self.local_address = response_data.get("assigned_address")
                    log.info(f"IPCP '{self.app_name}': Enrollment successful (TID: {tid}). Assigned Address: {self.local_address}")

                    # --- Set Future Before Registration ---
                    log.debug(f"IPCP '{self.app_name}': Attempting to set result=True for enroll_future (ID: {id(enroll_future)}) TID {tid}")
                    enroll_future.set_result(True)
                    log.debug(f"IPCP '{self.app_name}': Successfully SET result for enroll_future (ID: {id(enroll_future)}) TID {tid}")
                    await asyncio.sleep(0) # Yield control
                    # Schedule registration concurrently
                    log.debug(f"IPCP '{self.app_name}': Scheduling registration with parent DIF object.")
                    asyncio.create_task(
                        self.parent_dif.register_ipcp(self.app_name, self),
                        name=f"register_{self.app_name}"
                    )
                else: # State was not ENROLLING
                     log.warning(f"IPCP '{self.app_name}': Received success response (TID: {tid}) but state is {self.state}. Signalling success anyway.")
                     log.debug(f"IPCP '{self.app_name}': Attempting to set result=True for enroll_future (ID: {id(enroll_future)}) TID {tid} (state mismatch)")
                     enroll_future.set_result(True)
                     set_success = True # Mark that we set success
                     log.debug(f"IPCP '{self.app_name}': Successfully SET result for enroll_future (ID: {id(enroll_future)}) TID {tid} (state mismatch)")
                     await asyncio.sleep(0) # Yield control
            else: # status != "success"
                log.error(f"IPCP '{self.app_name}': Enrollment failed by DIF (TID: {tid}). Reason: {reason}")
                if self.state == IPCProcessState.ENROLLING:
                     self.state = IPCProcessState.FAILED_ENROLLMENT
                log.debug(f"IPCP '{self.app_name}': Attempting to set result=False for enroll_future (ID: {id(enroll_future)}) TID {tid}")
                enroll_future.set_result(False) # Signal failure
                log.debug(f"IPCP '{self.app_name}': Successfully SET failure result for enroll_future (ID: {id(enroll_future)}) TID {tid}")
                await asyncio.sleep(0) # Yield control

        except asyncio.InvalidStateError:
            # This might happen if the future was cancelled (e.g., by timeout) just before set_result was called
            log.warning(f"IPCP '{self.app_name}': Future was already done when trying to set result (TID: {tid}). Might be due to timeout.")
        except (json.JSONDecodeError, ValueError, AttributeError, KeyError, TypeError) as e:
            log.error(f"IPCP '{self.app_name}': Failed to parse enrollment response PDU payload: {e} - PDU: {pdu}")
            if enroll_future and not enroll_future.done():
                 try: enroll_future.set_result(False) # Signal failure if possible
                 except asyncio.InvalidStateError: pass
                 await asyncio.sleep(0) # Yield
        except Exception as e:
             log.exception(f"IPCP '{self.app_name}': Unexpected error in _handle_enrollment_response (TID: {tid})")
             if enroll_future and not enroll_future.done():
                  try: enroll_future.set_result(False) # Signal failure
                  except asyncio.InvalidStateError: pass
                  await asyncio.sleep(0) # Yield

    async def unenroll(self):
        # ... (unchanged - still simplified) ...
        if self.state != IPCProcessState.ENROLLED: return
        log.info(f"IPCP '{self.app_name}': Unenrolling (local operation)...")
        self.state = IPCProcessState.UNENROLLING
        await self.parent_dif.unregister_ipcp(self.app_name)
        self.state = IPCProcessState.UNENROLLED
        log.info(f"IPCP '{self.app_name}': Unenrolled successfully (local).")


    # --- Flow Management ---
    async def allocate_flow(self, target_app_name: ApplicationName, qos_params: Optional[Dict] = None) -> Optional[PortId]:
        # ... (Similar structure to enroll: create future, send PDU, wait_for, finally block for cleanup) ...
        if self.state != IPCProcessState.ENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot allocate flow, not enrolled.")
            return None
        if not self._underlying_ipcps and not self.network_interface:
             log.error(f"IPCP '{self.app_name}': Cannot allocate flow, no underlying transport.")
             return None

        log.info(f"IPCP '{self.app_name}': Requesting flow to '{target_app_name}' via PDU...")
        qos = qos_params if qos_params else {}
        tid = random.randint(1, 2**31)

        request_payload = json.dumps({
            "source_app_name": str(self.app_name), "target_app_name": str(target_app_name),
            "qos_params": qos, "tid": tid
        }).encode('utf-8')

        flow_request_pdu = PDU(
            pdu_type=PduType.MGMT_FLOW_ALLOC_REQ, dest_port_id=PortId(0),
            src_port_id=PortId(0), payload=request_payload
        )
        target_service_address = DIF_FLOW_ALLOCATOR_ADDRESS

        flow_alloc_future = asyncio.Future()
        log.debug(f"IPCP '{self.app_name}': Created flow_alloc_future (ID: {id(flow_alloc_future)}) for TID {tid}")
        async with self._request_lock:
            self._pending_requests[tid] = flow_alloc_future

        result_port_id: Optional[PortId] = None # Default to failure (None)
        try:
            sent = await self._send_on_underlying(flow_request_pdu, target_service_address)
            if not sent:
                if not flow_alloc_future.done(): flow_alloc_future.set_result(None)
                raise ConnectionError("Failed to send flow allocation request PDU.")

            log.debug(f"IPCP '{self.app_name}': Flow request sent (TID: {tid}), waiting {REQUEST_TIMEOUT}s...")
            result_port_id = await asyncio.wait_for(flow_alloc_future, timeout=REQUEST_TIMEOUT)
            log.info(f"IPCP '{self.app_name}': Flow allocation result received (TID: {tid}): PortId={result_port_id}")

        except asyncio.TimeoutError:
            log.error(f"IPCP '{self.app_name}': Flow allocation request (TID: {tid}) timed out.")
            # result_port_id remains None
        except Exception as e:
            log.error(f"IPCP '{self.app_name}': Exception during flow allocation PDU handling (TID: {tid}): {e}", exc_info=True)
            # result_port_id remains None
        finally:
            # --- Cleanup for flow allocation ---
            async with self._request_lock:
                removed_future = self._pending_requests.pop(tid, None)
                if removed_future and not removed_future.done() and result_port_id is None:
                     log.warning(f"IPCP '{self.app_name}': Removed pending flow alloc future (TID: {tid}) that failed without being resolved.")
            # --- End Cleanup ---
        return result_port_id

    # --- Make handler async ---
    async def _handle_flow_allocation_response(self, pdu: PDU):
        """Processes an MGMT_FLOW_ALLOC_RESP PDU."""
        tid = None # Initialize for exception block
        flow_alloc_future = None
        result_to_set: Optional[PortId] = None # Default to failure

        try:
            response_data = json.loads(pdu.payload.decode('utf-8'))
            tid = response_data.get("tid")
            status = response_data.get("status")
            reason = response_data.get("reason")
            flow_info = response_data.get("flow_info", {})
            if tid is None: raise ValueError("Missing tid in response payload")

            async with self._request_lock:
                flow_alloc_future = self._pending_requests.get(tid)
                if flow_alloc_future: log.debug(f"IPCP '{self.app_name}': Found pending flow_alloc_future (ID: {id(flow_alloc_future)}) for TID {tid}. Done={flow_alloc_future.done()}")
                else: log.warning(f"IPCP '{self.app_name}': No pending future found for TID {tid} in flow handler.")

            if not flow_alloc_future or flow_alloc_future.done():
                log.warning(f"IPCP '{self.app_name}': Ignoring flow response for TID {tid} - no pending/valid future found.")
                return

            log.debug(f"IPCP '{self.app_name}': Handling flow alloc response for TID {tid}. Status: {status}")

            if status == "success":
                try:
                    local_port_id = PortId(flow_info["source_port_id"])
                    remote_port_id = PortId(flow_info["destination_port_id"])
                    target_app_name = ApplicationName(flow_info["target_app_name"])
                    negotiated_qos = flow_info.get("negotiated_qos", {})

                    log.info(f"IPCP '{self.app_name}': Flow allocation success details received (TID: {tid}). Local:{local_port_id}, Remote:{remote_port_id}.")

                    # Ensure Flow class is available
                    try: from .flow import Flow
                    except ImportError: from rina.core.flow import Flow
                    new_flow = Flow(...) # Args omitted for brevity
                    await self._register_flow(local_port_id, new_flow)
                    result_to_set = local_port_id # Set PortId on success

                except (TypeError, ValueError, KeyError) as e:
                     log.error(f"IPCP '{self.app_name}': Invalid flow info in successful response (TID: {tid}): {e}")
                except Exception as reg_err: # Catch potential errors during _register_flow
                     log.error(f"IPCP '{self.app_name}': Error registering flow after allocation (TID: {tid}): {reg_err}", exc_info=True)
            else: # status != "success"
                log.error(f"IPCP '{self.app_name}': Flow allocation failed by DIF (TID: {tid}). Reason: {reason}")

            # Set future result
            log.debug(f"IPCP '{self.app_name}': Attempting to set result ({result_to_set}) for flow_alloc_future (ID: {id(flow_alloc_future)}) TID {tid}")
            flow_alloc_future.set_result(result_to_set)
            log.debug(f"IPCP '{self.app_name}': Successfully SET result for flow_alloc_future (ID: {id(flow_alloc_future)}) TID {tid}")
            await asyncio.sleep(0) # Yield control


        except asyncio.InvalidStateError:
             log.warning(f"IPCP '{self.app_name}': Flow alloc future was already done when trying set result (TID: {tid}). Might be timeout.")
        except (json.JSONDecodeError, ValueError, AttributeError, KeyError, TypeError) as e:
            log.error(f"IPCP '{self.app_name}': Failed to parse flow alloc response PDU payload: {e} - PDU: {pdu}")
            if flow_alloc_future and not flow_alloc_future.done():
                 try: flow_alloc_future.set_result(None) # Signal failure
                 except asyncio.InvalidStateError: pass
                 await asyncio.sleep(0) # Yield
        except Exception as e:
             log.exception(f"IPCP '{self.app_name}': Unexpected error in _handle_flow_allocation_response (TID: {tid})")
             if flow_alloc_future and not flow_alloc_future.done():
                  try: flow_alloc_future.set_result(None) # Signal failure
                  except asyncio.InvalidStateError: pass
                  await asyncio.sleep(0) # Yield

    # --- Make helper async ---
    async def _register_flow(self, port_id: PortId, flow: 'Flow'):
        # ... (unchanged) ...
        async with self._flows_lock:
            if port_id in self._flows:
                log.warning(f"IPCP '{self.app_name}': PortId {port_id} already exists. Overwriting.")
            self._flows[port_id] = flow


    async def deallocate_flow(self, port_id: PortId, notify_peer: bool = True):
        # ... (unchanged) ...
        async with self._flows_lock: flow_to_remove = self._flows.pop(port_id, None)
        if not flow_to_remove: return
        log.info(f"IPCP '{self.app_name}': Deallocating flow {port_id} to '{flow_to_remove.peer_app_name}' (local)...")
        await flow_to_remove.close(graceful=notify_peer)
        log.info(f"IPCP '{self.app_name}': Flow {port_id} deallocated (local).")


    # --- Data Transfer ---
    async def send_data(self, port_id: PortId, data: bytes) -> bool:
        # ... (unchanged) ...
        if self.state != IPCProcessState.ENROLLED: log.warning(f"IPCP '{self.app_name}': Cannot send, not enrolled."); return False
        async with self._flows_lock: flow = self._flows.get(port_id)
        if not flow: log.error(f"IPCP '{self.app_name}': Cannot send, PortId {port_id} not active."); return False
        try: pdus_to_send: List[PDU] = flow.prepare_data_pdus(data)
        except Exception as e: log.error(f"IPCP '{self.app_name}' Flow {port_id}: Error preparing data PDUs: {e}", exc_info=True); return False
        if not pdus_to_send: return True
        all_sent = True
        tasks = [asyncio.create_task(self._send_on_underlying(pdu), name=f"send_pdu_{pdu.seq_num}") for pdu in pdus_to_send]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, res in enumerate(results):
             if isinstance(res, Exception) or res is False:
                  all_sent = False; log.error(f"IPCP '{self.app_name}' Flow {port_id}: Failed send PDU {pdus_to_send[i].seq_num}. Error: {res}")
        return all_sent

    # --- Make helpers async ---
    async def _get_peer_underlying_address(self, dest_port_id: PortId) -> Optional[UnderlyingAddress]:
         """Resolves flow peer's underlying address via LayerManager (Async)."""
         async with self._flows_lock: flow = self._flows.get(dest_port_id)
         if flow: return await self.layer_manager.resolve_app_to_underlying_address(flow.peer_app_name, self)
         else: log.warning(f"IPCP '{self.app_name}': Cannot find flow {dest_port_id} to resolve addr."); return None

    async def _resolve_service_address(self, service_name: ApplicationName) -> Optional[UnderlyingAddress]:
         """Resolves DIF service's underlying address via LayerManager (Async)."""
         return await self.layer_manager.resolve_service_to_underlying_address(service_name, self.parent_dif, self)

    async def _send_on_underlying(self, pdu: PDU, target_address: Optional[Union[ApplicationName, Any]] = None) -> bool:
        """Sends a PDU using the configured underlying transport, resolving address if needed."""
        pdu_bytes = pdu.to_bytes();
        if not pdu_bytes: log.error(f"IPCP '{self.app_name}': Failed serialize PDU {pdu}."); return False
        # Shorten log message slightly
        log.debug(f"IPCP '{self.app_name}': Sending {pdu.pdu_type.name} Downwards (Targ:{target_address or pdu.dest_port_id})...")
        underlying_dest: Optional[UnderlyingAddress] = None
        if isinstance(target_address, ApplicationName): underlying_dest = await self._resolve_service_address(target_address)
        elif target_address: underlying_dest = target_address
        elif pdu.dest_port_id != 0: underlying_dest = await self._get_peer_underlying_address(pdu.dest_port_id)
        else: log.error(f"IPCP '{self.app_name}': Cannot determine dest for {pdu}"); return False
        if not underlying_dest: log.error(f"IPCP '{self.app_name}': Failed resolve underlying dest for Targ:{target_address or pdu.dest_port_id}."); return False

        # Send via Network Interface
        if self.network_interface:
            log.debug(f"IPCP '{self.app_name}': Sending {len(pdu_bytes)}b via IF '{self.network_interface_id}' to {underlying_dest}")
            try:
                if not isinstance(underlying_dest, tuple): log.error(f"IPCP '{self.app_name}': Invalid addr format for IF: {underlying_dest}"); return False
                return await self.network_interface.send(pdu_bytes, underlying_dest)
            except Exception as e: log.error(f"IPCP '{self.app_name}': IF send error to {underlying_dest}: {e}", exc_info=True); return False
        # Send via Underlying IPCP
        elif self._underlying_ipcps:
            underlying_ipcp = next(iter(self._underlying_ipcps.values()), None)
            if underlying_ipcp:
                log.debug(f"IPCP '{self.app_name}': Relaying PDU via underlying IPCP '{underlying_ipcp.app_name}'")
                source_addr = await self.layer_manager.resolve_app_to_underlying_address(self.app_name, self) or f"{self.app_name}_in_{underlying_ipcp.parent_dif.dif_name}"
                asyncio.create_task(underlying_ipcp.receive_pdu(pdu_bytes, source_addr))
                return True # Assume async success
            else: log.error(f"IPCP '{self.app_name}': No underlying IPCP available."); return False
        else: log.error(f"IPCP '{self.app_name}': No underlying transport configured."); return False

    async def receive_pdu(self, pdu_bytes: bytes, source_underlying_address: UnderlyingAddress):
        log.debug(f"IPCP '{self.app_name}': Rcvd {len(pdu_bytes)}b from {source_underlying_address}.")
        pdu = PDU.from_bytes(pdu_bytes)
        if not pdu: log.warning(f"IPCP '{self.app_name}': Discarding malformed PDU from {source_underlying_address}."); return
        log.debug(f"IPCP '{self.app_name}': Parsed PDU: {pdu}")

        # --- Forward MGMT REQ if proxy ---
        is_node_for_dif_mgmt = (self.parent_dif.management_transport == self)
        if is_node_for_dif_mgmt and pdu.pdu_type in [PduType.MGMT_ENROLL_REQ, PduType.MGMT_FLOW_ALLOC_REQ, PduType.MGMT_FLOW_DEALLOC]:
            log.info(f"IPCP '{self.app_name}' (proxy): Forwarding {pdu.pdu_type.name} PDU up to DIF '{self.parent_dif.dif_name}'")
            asyncio.create_task(self.parent_dif.receive_management_pdu(pdu, source_underlying_address))
            return

        is_mgmt_response = False
        handler = None  # Initialize to None
        if pdu.pdu_type in [PduType.MGMT_ENROLL_RESP, PduType.MGMT_FLOW_ALLOC_RESP]:
            try:
                response_data = json.loads(pdu.payload.decode('utf-8'))
                tid = response_data.get("tid")
                if isinstance(tid, int):
                    async with self._request_lock:
                        if tid in self._pending_requests:
                            is_mgmt_response = True
                            log.debug(f"IPCP '{self.app_name}': Rcvd {pdu.pdu_type.name} matching TID {tid}.")
                            # Assign handler based on PDU type
                            if pdu.pdu_type == PduType.MGMT_ENROLL_RESP:
                                handler = self._handle_enrollment_response(pdu)
                            elif pdu.pdu_type == PduType.MGMT_FLOW_ALLOC_RESP:
                                handler = self._handle_flow_allocation_response(pdu)
            except Exception as e:
                log.error(f"IPCP '{self.app_name}': Failed to process MGMT RESP: {e}")

        # Execute handler if assigned
        if handler:
            await handler
            return  # Exit after handling

        # --- Handle Flow PDUs (skip if PortId=0) ---
        if pdu.dest_port_id == 0:
            log.warning(f"IPCP '{self.app_name}': Discarding PDU for PortId 0: {pdu}")
            return

        # --- Handle Flow PDU ---
        if not is_mgmt_response:
            if pdu.dest_port_id == 0: log.warning(f"IPCP '{self.app_name}': Rcvd PDU for PortId 0, not handled. PDU: {pdu}. Discarding."); return
            async with self._flows_lock: flow = self._flows.get(pdu.dest_port_id)
            if not flow: log.warning(f"IPCP '{self.app_name}': Rcvd PDU for unknown PortId {pdu.dest_port_id} from {source_underlying_address}. PDU: {pdu}. Discarding."); return
            try: await flow.process_received_pdu(pdu)
            except Exception as e: log.error(f"IPCP '{self.app_name}' Flow {pdu.dest_port_id}: Error processing PDU: {e}", exc_info=True)

    async def deliver_to_app(self, port_id: PortId, data: bytes):
        # ... (unchanged) ...
        if self.app_handle:
            log.debug(f"IPCP '{self.app_name}': Delivering {len(data)}b for PortId {port_id} to app.")
            try:
                if asyncio.iscoroutinefunction(self.app_handle): await self.app_handle(port_id, data)
                else: log.warning(f"IPCP '{self.app_name}': App handle not async."); self.app_handle(port_id, data)
            except Exception as e: log.error(f"IPCP '{self.app_name}': App handle exception: {e}", exc_info=True)
        else: log.warning(f"IPCP '{self.app_name}': No app handle for PortId {port_id}.")


    def __repr__(self) -> str:
        # ... (unchanged) ...
        transport = f"IF:{self.network_interface_id}" if self.network_interface else f"IPCPs:{list(self._underlying_ipcps.keys())}"
        return (f"IPCProcess(app='{self.app_name}', state={self.state.name}, " f"dif='{self.parent_dif.dif_name}', transport={transport})")

# Ensure IPCProcessState is available at module level if needed elsewhere implicitly
try: _ = IPCProcessState.NULL
except NameError:
    class IPCProcessState(Enum):
        NULL = auto()
        UNENROLLED = auto()
        ENROLLING = auto()
        ENROLLED = auto()
        FAILED_ENROLLMENT = auto()
        UNENROLLING = auto()