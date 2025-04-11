import asyncio
import logging
from enum import Enum, auto
from typing import Dict, Optional, TYPE_CHECKING, List

from .naming import ApplicationName, PortId
# Import the PDU object definition
from .pdu import PDU, PduType, PduFlags

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    # Import IPCProcess with corrected path if needed (adjust '..' based on your structure)
    from .ipcp import IPCProcess

# Simplified Flow State Machine
class FlowState(Enum):
    CLOSED = auto()
    ESTABLISHED = auto()
    FIN_WAIT_1 = auto()
    FIN_WAIT_2 = auto()
    CLOSE_WAIT = auto()
    CLOSING = auto()
    LAST_ACK = auto()
    TIME_WAIT = auto()

DEFAULT_RETRANSMISSION_TIMEOUT = 1.0 # seconds
MAX_RETRIES = 5

class Flow:
    """
    Represents an active communication flow between two IPCPs.

    Manages flow state, data transfer protocol (simplified reliable - Stop-and-Wait),
    and interaction with the parent IPCP. Generates PDUs for sending and processes
    received PDUs. Manages retransmission timers for its own reliability.
    """
    def __init__(self,
                 local_ipcp: 'IPCProcess',
                 port_id: PortId,
                 peer_app_name: ApplicationName,
                 peer_port_id: PortId,
                 qos_params: Dict):
        self.local_ipcp: 'IPCProcess' = local_ipcp
        self.port_id: PortId = port_id
        self.peer_app_name: ApplicationName = peer_app_name
        self.peer_port_id: PortId = peer_port_id
        self.qos_params: Dict = qos_params
        self.state: FlowState = FlowState.ESTABLISHED # Assume established by allocation

        # Simplified Protocol State (Stop-and-Wait like)
        self.send_seq_num: int = 0       # Next sequence number to send
        self.last_ack_received: int = -1 # Sequence number of the last DATA PDU acked by peer
        self.receive_seq_num: int = 0    # Next data sequence number expected from peer
        self.last_data_pdu_sent: Optional[PDU] = None # Store the actual PDU for retransmission
        self.retry_count: int = 0
        self.retransmission_timer: Optional[asyncio.TimerHandle] = None

        # Buffer for potential future use (e.g., sliding window, reassembly)
        self.receive_buffer: Dict[int, bytes] = {} # Key: SeqNum, Value: Payload

        log.info(f"Flow {self.port_id} created for IPCP '{self.local_ipcp.app_name}' "
                 f"to peer '{self.peer_app_name}:{self.peer_port_id}'. State: {self.state}")

    # Removed internal _send_pdu method. Sending is delegated to IPCProcess._send_on_underlying

    def _start_retransmission_timer(self):
        """Starts the retransmission timer for the last sent DATA PDU."""
        self._cancel_retransmission_timer()
        if self.last_data_pdu_sent: # Only start if there's something to potentially retransmit
            loop = asyncio.get_running_loop()
            self.retransmission_timer = loop.call_later(
                DEFAULT_RETRANSMISSION_TIMEOUT, self._handle_timeout
            )
            log.debug(f"Flow {self.port_id}: Retransmission timer started for Seq={self.last_data_pdu_sent.seq_num}.")
        else:
             log.warning(f"Flow {self.port_id}: Tried to start retransmission timer, but no last_data_pdu_sent.")


    def _cancel_retransmission_timer(self):
        """Cancels the active retransmission timer."""
        if self.retransmission_timer:
            self.retransmission_timer.cancel()
            self.retransmission_timer = None
            log.debug(f"Flow {self.port_id}: Retransmission timer cancelled.")

    def _handle_timeout(self):
        """
        Handles retransmission timer expiration.
        Retries sending the last unacknowledged DATA PDU via the parent IPCP.
        """
        self.retransmission_timer = None # Timer handle is used

        if self.last_data_pdu_sent and self.state in [FlowState.ESTABLISHED]: # Check state allows sending
            # Check if the PDU was actually acknowledged while timer was pending (race condition)
            if self.last_ack_received >= self.last_data_pdu_sent.seq_num:
                 log.debug(f"Flow {self.port_id}: Timeout handled, but PDU Seq={self.last_data_pdu_sent.seq_num} already ACKed ({self.last_ack_received}).")
                 return

            if self.retry_count < MAX_RETRIES:
                self.retry_count += 1
                log.warning(f"Flow {self.port_id}: Timeout! Retrying PDU Seq={self.last_data_pdu_sent.seq_num} (Attempt {self.retry_count})")
                # Resend by calling the IPCP's send method directly
                # Use asyncio.create_task as _send_on_underlying is async
                asyncio.create_task(self.local_ipcp._send_on_underlying(self.last_data_pdu_sent))
                # Restart the timer after attempting resend
                self._start_retransmission_timer()
            else:
                log.error(f"Flow {self.port_id}: Max retries exceeded for PDU Seq={self.last_data_pdu_sent.seq_num}. Closing flow.")
                self.last_data_pdu_sent = None # Give up sending this PDU
                # Handle connection failure - close the flow abruptly
                asyncio.create_task(self.close(graceful=False))
        else:
             log.debug(f"Flow {self.port_id}: Timeout handled, but no PDU to retransmit or wrong state ({self.state}).")


    def prepare_data_pdus(self, data: bytes) -> List[PDU]:
        """
        Prepares Data PDU(s) for sending based on protocol state (Stop-and-Wait).
        Returns a list containing the PDU object to be sent, or an empty list if unable/unwilling to send.
        """
        if self.state != FlowState.ESTABLISHED:
            log.warning(f"Flow {self.port_id}: Cannot prepare data, flow not established (State: {self.state}).")
            return []

        # Stop-and-Wait: Only prepare new PDU if the previous one has been ACKed
        if self.last_data_pdu_sent and self.last_ack_received < self.last_data_pdu_sent.seq_num:
             log.debug(f"Flow {self.port_id}: Cannot prepare new data yet, waiting for ACK of Seq={self.last_data_pdu_sent.seq_num}")
             # In stop-and-wait, we don't buffer outgoing; caller should retry later or buffer itself.
             return []
        if not data:
             log.debug(f"Flow {self.port_id}: No data provided to send.")
             return []

        # Create the DATA PDU object
        pdu = PDU(
            pdu_type=PduType.DATA,
            dest_port_id=self.peer_port_id,
            src_port_id=self.port_id,
            seq_num=self.send_seq_num,
            ack_num=self.receive_seq_num, # Piggyback latest ACK number
            flags=PduFlags.ACK_FLAG,     # Indicate ACK number is valid
            payload=data
        )

        # Store this PDU in case retransmission is needed
        self.last_data_pdu_sent = pdu
        self.retry_count = 0 # Reset retry count for new PDU
        # Start the retransmission timer *after* returning the PDU to the IPCP for sending attempt.
        # The IPCP should ideally call back or signal success/failure of the send attempt.
        # Simplification: Start timer optimistically here.
        self._start_retransmission_timer()

        # Increment sequence number for the *next* PDU
        self.send_seq_num += 1

        # Return the PDU object for the IPCP to send
        return [pdu]


    async def process_received_pdu(self, pdu: PDU):
        """
        Processes a received PDU object according to protocol rules.
        Handles ACKs, DATA, and FINs. Calls IPCP to deliver data to app.
        Sends necessary ACKs via parent IPCP.
        """
        # pdu object is now passed directly
        if not isinstance(pdu, PDU):
             log.error(f"Flow {self.port_id}: process_received_pdu called with non-PDU object: {type(pdu)}")
             return

        log.debug(f"Flow {self.port_id}: Processing received PDU: {pdu}")

        # --- ACK Processing (for our outgoing DATA) ---
        if pdu.flags & PduFlags.ACK_FLAG:
            ack_num_received = pdu.ack_num # Ack num from peer acknowledges data *we* sent
            # Check if this acknowledges our outstanding DATA PDU
            if self.last_data_pdu_sent and ack_num_received == self.last_data_pdu_sent.seq_num:
                 log.debug(f"Flow {self.port_id}: Received ACK for our sent Seq={ack_num_received}. Previous flow ACK was {self.last_ack_received}")
                 self._cancel_retransmission_timer()
                 self.last_ack_received = ack_num_received # Update last ACK for data we sent
                 self.last_data_pdu_sent = None # Clear the PDU needing ACK
                 # If we were buffering outgoing data, now would be the time to send the next segment
            elif ack_num_received > self.last_ack_received:
                 # Peer might be ACKing something further (if using sliding window later)
                 # or it could be an ACK for a FIN. Update if it's higher.
                 log.debug(f"Flow {self.port_id}: Received ACK for {ack_num_received}, higher than current {self.last_ack_received}. Updating.")
                 self.last_ack_received = ack_num_received
                 # If this ACK covers the last sent PDU, also cancel timer
                 if self.last_data_pdu_sent and ack_num_received >= self.last_data_pdu_sent.seq_num:
                      self._cancel_retransmission_timer()
                      self.last_data_pdu_sent = None
            # Ignore ACKs for sequence numbers we already know are ACKed


        # --- DATA Processing (incoming DATA from peer) ---
        if pdu.pdu_type == PduType.DATA:
            data_seq_num = pdu.seq_num
            # Check if it's the sequence number we expect (in-order)
            if data_seq_num == self.receive_seq_num:
                log.debug(f"Flow {self.port_id}: Received expected DATA Seq={data_seq_num}")
                # Update expected sequence number for the *next* PDU from peer
                self.receive_seq_num += 1
                # Process payload (deliver to application)
                if pdu.payload:
                    await self.local_ipcp.deliver_to_app(self.port_id, pdu.payload)
                # Acknowledge receipt of this DATA PDU
                await self._send_ack()

                # Check receive buffer for contiguous data (if buffering implemented later)

            elif data_seq_num < self.receive_seq_num:
                log.warning(f"Flow {self.port_id}: Received duplicate DATA Seq={data_seq_num} (expected {self.receive_seq_num}). Sending ACK.")
                # Resend ACK for the highest sequence number *successfully received* so far
                await self._send_ack()
            else: # data_seq_num > self.receive_seq_num
                log.warning(f"Flow {self.port_id}: Received out-of-order DATA Seq={data_seq_num} (expected {self.receive_seq_num}). Discarding (simple protocol).")
                # In a more complex protocol: Buffer out-of-order data here (self.receive_buffer[data_seq_num] = pdu.payload)
                # Always send ACK for the last *in-order* sequence number received to help peer resend missing data.
                await self._send_ack()

        # --- FIN Processing (incoming FIN from peer) ---
        if pdu.flags & PduFlags.FIN_FLAG:
            log.info(f"Flow {self.port_id}: Received FIN flag from peer (Seq={pdu.seq_num}). Current state: {self.state}")
            # TODO: Implement proper FIN state transitions based on current state (e.g., ESTABLISHED -> CLOSE_WAIT)
            # Acknowledge the FIN. The ACK number should cover the FIN sequence number if FIN consumes one.
            # Let's assume FIN doesn't consume a sequence number in this simple model.
            if self.state in [FlowState.ESTABLISHED, FlowState.FIN_WAIT_1, FlowState.FIN_WAIT_2]: # Check states where receiving FIN is expected
                 # Acknowledge receipt of peer's FIN flag
                 await self._send_ack()
                 # Update state (Simplified: move towards closed)
                 if self.state == FlowState.ESTABLISHED:
                     self.state = FlowState.CLOSE_WAIT # We received FIN first
                     # Application should be notified to close its end
                 elif self.state == FlowState.FIN_WAIT_1:
                      # We sent FIN, they sent FIN. If our FIN is ACKed (via pdu.ack_num?), move to TIME_WAIT/CLOSED.
                      # Simplified: Assume simultaneous close, move to CLOSING or LAST_ACK based on who ACKs first.
                      self.state = FlowState.CLOSING # Or TIME_WAIT? Needs clearer state machine rules.
                 elif self.state == FlowState.FIN_WAIT_2:
                      # We sent FIN, got ACK, now got peer FIN. Move to TIME_WAIT.
                      self.state = FlowState.TIME_WAIT
                      # Start TIME_WAIT timer here if implementing it.
                 log.info(f"Flow {self.port_id}: Processed received FIN. New state: {self.state}")
                 self._cancel_retransmission_timer() # Stop data retransmissions if closing
            else:
                 log.warning(f"Flow {self.port_id}: Received FIN in unexpected state {self.state}. Sending ACK anyway.")
                 await self._send_ack() # Acknowledge FIN even if state is odd


    async def _send_ack(self):
        """Creates an ACK PDU and sends it via the parent IPCP."""
        ack_pdu = PDU(
            pdu_type=PduType.ACK,
            dest_port_id=self.peer_port_id,
            src_port_id=self.port_id,
            seq_num=self.send_seq_num, # Our current send sequence number (might be unused by peer for pure ACK)
            ack_num=self.receive_seq_num, # Acknowledge data received up to this sequence number from peer
            flags=PduFlags.ACK_FLAG,
            payload=b''
        )
        # Send immediately using parent IPCP - use create_task to avoid blocking current PDU processing
        # ACKs themselves are typically not reliably sent (i.e., not timed/retried)
        log.debug(f"Flow {self.port_id}: Sending ACK PDU (AckNum: {ack_pdu.ack_num})")
        asyncio.create_task(self.local_ipcp._send_on_underlying(ack_pdu))


    async def close(self, graceful=True):
        """Initiates flow termination, potentially sending a FIN PDU via parent IPCP."""
        log.info(f"Flow {self.port_id}: Initiating close (graceful={graceful}). Current state: {self.state}")
        if self.state in [FlowState.CLOSED, FlowState.FIN_WAIT_1, FlowState.CLOSING, FlowState.LAST_ACK, FlowState.TIME_WAIT]: # Add CLOSE_WAIT? If app initiates close from CLOSE_WAIT, send FIN.
            if self.state != FlowState.CLOSE_WAIT: # Allow transition from CLOSE_WAIT to LAST_ACK by sending FIN
                 log.warning(f"Flow {self.port_id}: Already closing or closed (State: {self.state}).")
                 return

        # Cancel data retransmission timer if active
        self._cancel_retransmission_timer()
        self.last_data_pdu_sent = None # Stop trying to resend data

        original_state = self.state

        if graceful:
            # Send FIN PDU
            fin_pdu = PDU(
                pdu_type=PduType.FIN, # Can also use DATA type with FIN flag
                dest_port_id=self.peer_port_id,
                src_port_id=self.port_id,
                seq_num=self.send_seq_num, # FIN might consume a sequence number in some protocols
                ack_num=self.receive_seq_num, # Piggyback latest ACK
                flags=PduFlags.FIN_FLAG | PduFlags.ACK_FLAG,
                payload=b''
            )
            log.debug(f"Flow {self.port_id}: Sending FIN PDU (Seq={fin_pdu.seq_num})")
            sent = await self.local_ipcp._send_on_underlying(fin_pdu)
            # Assume FIN sending doesn't use retransmission timer like data for simplicity now
            # TODO: Reliable FIN sending might require its own timer/retry logic

            if sent:
                 # Update state based on state before sending FIN
                 if original_state == FlowState.ESTABLISHED:
                      self.state = FlowState.FIN_WAIT_1
                 elif original_state == FlowState.CLOSE_WAIT:
                      self.state = FlowState.LAST_ACK # Sent our FIN after receiving peer's FIN
                 # Increment seq num if FIN consumes one
                 # self.send_seq_num += 1
            else:
                 log.error(f"Flow {self.port_id}: Failed to send FIN PDU. Aborting close.")
                 # Revert state? Or move directly to CLOSED?
                 self.state = FlowState.CLOSED # Abort to closed state on send failure

        else: # Abrupt close
            self.state = FlowState.CLOSED
            # Optionally send a Reset PDU type if defined

        log.info(f"Flow {self.port_id}: Close initiated. New state: {self.state}")

    def __repr__(self) -> str:
         # Added last_ack_received for debugging
         return (f"Flow(Port:{self.port_id}, Peer:'{self.peer_app_name}:{self.peer_port_id}', "
                 f"State:{self.state.name}, NextSend:{self.send_seq_num}, NextRecv:{self.receive_seq_num}, LastAckRcvd:{self.last_ack_received})")