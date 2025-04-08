import asyncio
import logging
from enum import Enum, auto
from typing import Dict, Optional, Any, TYPE_CHECKING, Callable

from .naming import ApplicationName, PortId, DIFName

# Setup logger for this module
log = logging.getLogger(__name__)

# Avoid circular imports for type hints
if TYPE_CHECKING:
    from .dif import DIF
    from .flow import Flow
    from ..management.layer_manager import LayerManager
    # Define a type alias for the application callback/handle
    # This could be an asyncio.Queue, a callback function, or an object reference
    ApplicationHandle = Callable[[PortId, bytes], asyncio.Future] # Example: async def callback(port_id, data)

class IPCProcessState(Enum):
    """Represents the enrollment state of an IPC Process within a DIF."""
    NULL = auto()          # Initial state
    ENROLLING = auto()     # Enrollment initiated
    ENROLLED = auto()      # Successfully enrolled in the parent DIF
    FAILED_ENROLLMENT = auto() # Enrollment failed
    UNENROLLING = auto()   # De-enrollment initiated
    UNENROLLED = auto()    # Not enrolled (after successful unenrollment or never enrolled)

class IPCProcess:
    """
    Represents an IPC Process (IPCP).

    An active component within a DIF that manages communication flows
    on behalf of an Application Process or another DIF's management entity.
    It handles enrollment, flow management, and data transfer, potentially
    using services from IPCPs in underlying DIFs.
    """

    def __init__(self,
                 app_name: ApplicationName,
                 parent_dif: 'DIF',
                 layer_manager: 'LayerManager',
                 app_handle: Optional['ApplicationHandle'] = None):
                 # config: Optional[Dict[str, Any]] = None): # Optional config if needed
        """
        Initializes an IPC Process.

        Args:
            app_name: The unique Application Name this IPCP represents.
            parent_dif: The DIF this IPCP is intended to be a member of.
            layer_manager: The LayerManager instance.
            app_handle: A handle (e.g., async callback, queue) to deliver
                        received data to the associated Application Process.
        """
        self.app_name: ApplicationName = app_name
        self.parent_dif: 'DIF' = parent_dif
        self.layer_manager: 'LayerManager' = layer_manager
        self.app_handle: Optional['ApplicationHandle'] = app_handle
        # self.config = config if config else {}

        self.state: IPCProcessState = IPCProcessState.NULL
        self.local_address: Optional[str] = None # DIF-specific address, might be assigned on enrollment

        # Manages active flows associated with this IPCP. Key: PortId
        self._flows: Dict[PortId, 'Flow'] = {}
        self._flows_lock = asyncio.Lock() # Protect access to flows dict

        # Stores references to the IPCPs in underlying DIFs used for transport.
        # Key: Underlying DIF Name, Value: Underlying IPCProcess instance
        # This needs careful management during DIF setup/layering
        self._underlying_ipcps: Dict[DIFName, 'IPCProcess'] = {}

        log.info(f"IPCP for '{self.app_name}' created (State: {self.state}). Belongs to DIF '{self.parent_dif.dif_name}'.")

    async def start(self):
        """Starts the IPCP's background tasks, if any."""
        log.info(f"IPCP '{self.app_name}': Starting...")
        # TODO: Start any background tasks (e.g., monitoring app handle queue)
        self.state = IPCProcessState.UNENROLLED # Ready to enroll
        log.info(f"IPCP '{self.app_name}': Started (State: {self.state}).")

    async def stop(self):
        """Stops the IPCP, cleans up resources, and unenrolls."""
        log.info(f"IPCP '{self.app_name}': Stopping...")
        if self.state == IPCProcessState.ENROLLED:
            await self.unenroll()

        async with self._flows_lock:
            # Gracefully terminate all active flows
            for port_id in list(self._flows.keys()):
                await self.deallocate_flow(port_id, notify_peer=True) # Assuming deallocate handles cleanup
            self._flows.clear()

        self.state = IPCProcessState.NULL
        log.info(f"IPCP '{self.app_name}': Stopped (State: {self.state}).")

    def add_underlying_ipcp(self, dif_name: DIFName, ipcp: 'IPCProcess'):
        """Registers an IPCP in an underlying DIF used for transport."""
        log.info(f"IPCP '{self.app_name}': Adding underlying IPCP in DIF '{dif_name}'.")
        self._underlying_ipcps[dif_name] = ipcp
        # TODO: Need logic to determine *which* underlying IPCP to use for *which* destination

    # --- Enrollment ---

    async def enroll(self) -> bool:
        """
        Initiates the enrollment process for this IPCP into its parent DIF.
        """
        if self.state != IPCProcessState.UNENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot enroll, state is {self.state}.")
            return False

        log.info(f"IPCP '{self.app_name}': Attempting to enroll into DIF '{self.parent_dif.dif_name}'...")
        self.state = IPCProcessState.ENROLLING

        try:
            # In a real system, this requires:
            # 1. Discovering an enrollment point/agent for the parent DIF.
            #    (Could involve layer_manager or using underlying DIFs).
            # 2. Formatting an enrollment PDU.
            # 3. Sending the PDU using an underlying IPCP.
            # 4. Waiting for and processing the response PDU.

            # --- Placeholder Implementation ---
            # Assume we can directly call the DIF's handler for simulation.
            # We need info about ourselves and the network point we are reachable at.
            # This 'remote_address' would be from the perspective of the *underlying* DIF.
            request_info = {"app_name": self.app_name} # Add other required info
            remote_addr_placeholder = "underlying_dif_addr:1234" # Needs real context

            enrollment_response = await self.parent_dif.handle_enrollment_request(
                request_info, remote_addr_placeholder
            )
            # --- End Placeholder ---

            return await self._handle_enrollment_response(enrollment_response)

        except Exception as e:
            log.error(f"IPCP '{self.app_name}': Exception during enrollment: {e}", exc_info=True)
            self.state = IPCProcessState.FAILED_ENROLLMENT
            return False

    async def _handle_enrollment_response(self, response: Dict) -> bool:
        """Processes the response received from the DIF enrollment handler."""
        if response.get("status") == "success":
            log.info(f"IPCP '{self.app_name}': Enrollment successful into DIF '{self.parent_dif.dif_name}'.")
            self.state = IPCProcessState.ENROLLED
            # Store assigned address if provided: self.local_address = response.get("assigned_address")
            # Store credentials etc.
            # Now, officially register with the DIF object locally
            await self.parent_dif.register_ipcp(self.app_name, self)
            return True
        else:
            log.error(f"IPCP '{self.app_name}': Enrollment failed. Reason: {response.get('reason', 'Unknown')}")
            self.state = IPCProcessState.FAILED_ENROLLMENT
            return False

    async def unenroll(self):
        """Initiates de-enrollment from the parent DIF."""
        if self.state != IPCProcessState.ENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot unenroll, not enrolled.")
            return

        log.info(f"IPCP '{self.app_name}': Unenrolling from DIF '{self.parent_dif.dif_name}'...")
        self.state = IPCProcessState.UNENROLLING

        # TODO: Implement unenrollment protocol (send request to DIF, wait for ack)
        # Placeholder: Directly unregister locally
        await self.parent_dif.unregister_ipcp(self.app_name)
        self.state = IPCProcessState.UNENROLLED
        log.info(f"IPCP '{self.app_name}': Unenrolled successfully.")


    # --- Flow Management ---

    async def allocate_flow(self, target_app_name: ApplicationName, qos_params: Optional[Dict] = None) -> Optional[PortId]:
        """
        Requests the allocation of a new communication flow to a target application within the same DIF.

        Args:
            target_app_name: The name of the target application.
            qos_params: Desired Quality of Service parameters (optional).

        Returns:
            The assigned local PortId for the new flow if successful, otherwise None.
        """
        if self.state != IPCProcessState.ENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot allocate flow, not enrolled.")
            return None

        log.info(f"IPCP '{self.app_name}': Requesting flow to '{target_app_name}'...")
        qos = qos_params if qos_params else {}

        try:
            # Similar to enrollment, this involves:
            # 1. Finding the DIF's Flow Allocator service.
            # 2. Formatting a flow request PDU.
            # 3. Sending the PDU using an underlying IPCP.
            # 4. Processing the response.

            # --- Placeholder Implementation ---
            flow_response = await self.parent_dif.handle_flow_request(
                self.app_name, target_app_name, qos
            )
            # --- End Placeholder ---

            return await self._handle_flow_allocation_response(flow_response, target_app_name)

        except Exception as e:
            log.error(f"IPCP '{self.app_name}': Exception during flow allocation request: {e}", exc_info=True)
            return None

    async def _handle_flow_allocation_response(self, response: Dict, target_app_name: ApplicationName) -> Optional[PortId]:
        """Processes the response from the DIF's Flow Allocator."""
        if response.get("status") == "success":
            flow_info = response.get("flow_info", {})
            local_port_id = PortId(flow_info.get("source_port_id")) # Our end of the flow
            remote_port_id = PortId(flow_info.get("destination_port_id")) # Peer's end
            negotiated_qos = flow_info.get("negotiated_qos", {})

            if not local_port_id:
                 log.error(f"IPCP '{self.app_name}': Flow allocation response missing source_port_id.")
                 return None

            log.info(f"IPCP '{self.app_name}': Flow to '{target_app_name}' allocated. Local PortId: {local_port_id}, Remote PortId: {remote_port_id}.")

            # Create and store the Flow object
            from .flow import Flow # Local import to avoid circularity at module level
            new_flow = Flow(
                local_ipcp=self,
                port_id=local_port_id,
                peer_app_name=target_app_name,
                peer_port_id=remote_port_id,
                qos_params=negotiated_qos
            )
            async with self._flows_lock:
                self._flows[local_port_id] = new_flow

            # TODO: Perform any necessary data transfer protocol setup for the flow (e.g., RTT estimation)
            # await new_flow.initialize_protocol_state()

            return local_port_id
        else:
            log.error(f"IPCP '{self.app_name}': Flow allocation failed. Reason: {response.get('reason', 'Unknown')}")
            return None

    async def deallocate_flow(self, port_id: PortId, notify_peer: bool = True):
        """
        Requests the deallocation of an existing flow.

        Args:
            port_id: The local PortId of the flow to deallocate.
            notify_peer: Whether to attempt to notify the peer IPCP (via DIF management).
        """
        async with self._flows_lock:
            flow_to_remove = self._flows.pop(port_id, None)

        if not flow_to_remove:
            log.warning(f"IPCP '{self.app_name}': Cannot deallocate flow, PortId {port_id} not found.")
            return

        log.info(f"IPCP '{self.app_name}': Deallocating flow with PortId {port_id} to '{flow_to_remove.peer_app_name}'...")

        # TODO: Implement flow deallocation protocol:
        # 1. Send deallocation request PDU to DIF management/peer.
        # 2. Clean up internal flow state (done by removing from dict).
        # 3. Release associated resources.
        await flow_to_remove.close() # Assuming Flow object has cleanup logic

        log.info(f"IPCP '{self.app_name}': Flow {port_id} deallocated.")


    # --- Data Transfer ---

    async def send_data(self, port_id: PortId, data: bytes) -> bool:
        """
        Sends data over a specific allocated flow.

        Args:
            port_id: The local PortId identifying the flow.
            data: The application data payload (bytes).

        Returns:
            True if data was successfully processed for sending, False otherwise.
        """
        if self.state != IPCProcessState.ENROLLED:
            log.warning(f"IPCP '{self.app_name}': Cannot send data, not enrolled.")
            return False

        async with self._flows_lock:
            flow = self._flows.get(port_id)

        if not flow:
            log.error(f"IPCP '{self.app_name}': Cannot send data, PortId {port_id} does not correspond to an active flow.")
            return False

        # 1. Use the Flow object's data transfer protocol logic
        #    This might involve segmentation, adding sequence numbers, etc.
        #    It will produce one or more PDUs to be sent.
        try:
            pdus_to_send = await flow.prepare_data_pdus(data)
        except Exception as e:
             log.error(f"IPCP '{self.app_name}' Flow {port_id}: Error preparing data PDUs: {e}", exc_info=True)
             return False

        if not pdus_to_send:
             return True # Nothing to send, maybe flow control?

        # 2. Determine the underlying IPCP to use.
        #    This requires routing logic: How do we reach flow.peer_app_name via underlying DIFs?
        #    Simplification: Assume only one underlying IPCP or a simple static route.
        underlying_ipcp = next(iter(self._underlying_ipcps.values()), None)
        if not underlying_ipcp:
            log.error(f"IPCP '{self.app_name}': Cannot send data, no underlying IPCP configured.")
            return False

        # 3. Send the PDUs using the underlying IPCP's send mechanism.
        #    The underlying IPCP will encapsulate these PDUs further for its own DIF.
        success = True
        for pdu in pdus_to_send:
             # The 'send_data' of the underlying IPCP needs context about *its* flows.
             # This implies that when we created the flow here, we likely also
             # established corresponding flow(s) in the underlying DIF between the
             # underlying IPCPs involved. This is a key part of RINA recursion.
             # --- Placeholder: Assume underlying IPCP knows how to send raw bytes ---
             log.debug(f"IPCP '{self.app_name}': Sending PDU (size {len(pdu)}) via underlying IPCP '{underlying_ipcp.app_name}' in DIF '{underlying_ipcp.parent_dif.dif_name}'")
             # This needs a proper API - maybe send_pdu(target_peer_address_in_underlying_dif, pdu_bytes)
             # await underlying_ipcp.send_data(underlying_flow_id, pdu) # Requires mapping our flow to underlying flow
             pass # Placeholder - cannot implement fully without underlying send logic
             # Simulate success for now
             # if not await underlying_ipcp.some_send_method(pdu):
             #     success = False
             #     break

        return success # Represents successful handover to the next layer down


    async def receive_pdu(self, pdu: bytes, source_address_in_underlying_dif: Any):
        """
        Called by an underlying IPCP when it receives a PDU destined for this IPCP.
        This is the entry point for data coming *up* the stack.

        Args:
            pdu: The raw PDU bytes received from the lower layer.
            source_address_in_underlying_dif: Identifier of the sender in the context of the lower DIF.
        """
        log.debug(f"IPCP '{self.app_name}': Received PDU (size {len(pdu)}) from underlying DIF via {source_address_in_underlying_dif}.")

        # 1. Parse the PDU to extract destination PortId and other protocol info.
        #    This requires knowledge of the Data Transfer Protocol format.
        try:
            # --- Placeholder Parsing ---
            # Assume PDU structure: [DestPortId (int, 4 bytes)][Payload]
            if len(pdu) < 4:
                 log.warning(f"IPCP '{self.app_name}': Received PDU too short.")
                 return
            dest_port_id = PortId(int.from_bytes(pdu[:4], 'big'))
            payload = pdu[4:]
            # --- End Placeholder Parsing ---
            log.debug(f"IPCP '{self.app_name}': PDU destined for PortId {dest_port_id}.")

        except Exception as e:
            log.error(f"IPCP '{self.app_name}': Failed to parse received PDU: {e}", exc_info=True)
            return

        # 2. Find the corresponding Flow object.
        async with self._flows_lock:
            flow = self._flows.get(dest_port_id)

        if not flow:
            log.warning(f"IPCP '{self.app_name}': Received PDU for unknown/inactive PortId {dest_port_id}. Discarding.")
            # TODO: Optionally send back an error PDU (e.g., ICMP Port Unreachable equivalent)
            return

        # 3. Pass the PDU to the Flow object for processing by its Data Transfer Protocol state machine.
        #    This handles reassembly, acknowledgments, ordering, etc.
        #    The flow object will call self.deliver_to_app() when complete data is ready.
        try:
            await flow.process_received_pdu(pdu) # Pass the *whole* PDU for protocol processing
        except Exception as e:
             log.error(f"IPCP '{self.app_name}' Flow {dest_port_id}: Error processing received PDU: {e}", exc_info=True)


    async def deliver_to_app(self, port_id: PortId, data: bytes):
        """
        Delivers fully reassembled data payload to the associated Application Process.
        This method is typically called by a Flow object after its data transfer
        protocol has processed incoming PDUs.

        Args:
            port_id: The local PortId identifying the flow.
            data: The complete application data payload.
        """
        if self.app_handle:
            log.debug(f"IPCP '{self.app_name}': Delivering {len(data)} bytes for PortId {port_id} to application.")
            try:
                # Use the provided application handle (e.g., call the callback)
                # Ensure the callback is awaitable if it does I/O
                await self.app_handle(port_id, data)
            except Exception as e:
                log.error(f"IPCP '{self.app_name}': Application handle raised an exception: {e}", exc_info=True)
        else:
            log.warning(f"IPCP '{self.app_name}': No application handle configured to deliver data for PortId {port_id}.")

    def __repr__(self) -> str:
        return f"IPCProcess(app_name='{self.app_name}', state={self.state}, parent_dif='{self.parent_dif.dif_name}')"