import asyncio
import logging
from enum import Enum, auto
from typing import Dict, Optional, TYPE_CHECKING, List

from .naming import ApplicationName, PortId
from .pdu import PDU, PduType, PduFlags

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .ipcp import IPCProcess

# Simplified Flow State Machine
class FlowState(Enum):
    CLOSED = auto()
    # For simplicity, skipping explicit SYN states; assume established by allocation response
    ESTABLISHED = auto()
    FIN_WAIT_1 = auto() # Sent FIN, waiting for ACK/FIN
    FIN_WAIT_2 = auto() # Received ACK for our FIN, waiting for peer's FIN
    CLOSE_WAIT = auto() # Received FIN, will send ACK and then FIN
    CLOSING = auto()    # Received FIN, sent ACK, sent FIN, waiting for final ACK
    LAST_ACK = auto()   # Sent final FIN ACK, waiting briefly
    TIME_WAIT = auto()  # Waiting after close for delayed packets

DEFAULT_RETRANSMISSION_TIMEOUT = 1.0 # seconds
MAX_RETRIES = 5

class Flow:
    """
    Represents an active communication flow between two IPCPs.

    Manages flow state, data transfer protocol (simplified reliable),
    and interaction with the parent IPCP.
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
        self.last_ack_received: int = -1 # Highest sequence number acknowledged by peer
        self.receive_seq_num: int = 0    # Next sequence number expected from peer
        self.last_pdu_sent: Optional[PDU] = None
        self.retry_count: int = 0
        self.retransmission_timer: Optional[asyncio.TimerHandle] = None

        # Buffer for potential future use (e.g., sliding window)
        self.receive_buffer: Dict[int, bytes] = {}

        log.info(f"Flow {self.port_id} created for IPCP '{self.local_ipcp.app_name}' "
                 f"to peer '{self.peer_app_name}:{self.peer_port_id}'. State: {self.state}")

    async def _send_pdu(self, pdu: PDU):
        """Helper to send a PDU using the parent IPCP."""
        log.debug(f"Flow {self.port_id}: Sending PDU: {pdu}")
        # The IPCP's send_data needs the *flow's* port_id to know which flow context initiated the send.
        # However, the IPCP's send_data implementation will encapsulate this PDU
        # and send it using an *underlying* IPCP, which requires different context.
        # This highlights the need for a clear API in IPCProcess.send_data.
        # Let's assume IPCP.send_data knows how to handle this based on the flow object or port id.
        # For now, we directly call a hypothetical internal send method on the IPCP
        # which handles the underlying transmission.

        # --- Refined approach: Assume IPCP has _send_on_underlying(pdu_bytes) ---
        # This bypasses the IPCP.send_data() complexity for now.
        underlying_ipcp = next(iter(self.local_ipcp._underlying_ipcps.values()), None)
        if underlying_ipcp:
            # We need to tell the underlying IPCP where this PDU should go
            # in the *underlying* network. This requires routing info.
            # Placeholder: Assume underlying IPCP has a method send_raw(bytes, destination_info)
            # await underlying_ipcp.send_raw(pdu.to_bytes(), self.peer_app_name) # Needs proper routing!
            log.warning(f"Flow {self.port_id}: PDU sending logic placeholder - not actually sending.")
            pass # Placeholder - Actual sending needs routing via IPCProcess
        else:
            log.error(f"Flow {self.port_id}: Cannot send PDU, no underlying IPCP available.")

        # Simplified Stop-and-Wait: Set retransmission timer for DATA PDUs
        if pdu.pdu_type == PduType.DATA:
            self.last_pdu_sent = pdu
            self.retry_count = 0
            self._start_retransmission_timer()

    def _start_retransmission_timer(self):
        self._cancel_retransmission_timer()
        loop = asyncio.get_running_loop()
        self.retransmission_timer = loop.call_later(
            DEFAULT_RETRANSMISSION_TIMEOUT, self._handle_timeout
        )
        log.debug(f"Flow {self.port_id}: Retransmission timer started.")

    def _cancel_retransmission_timer(self):
        if self.retransmission_timer:
            self.retransmission_timer.cancel()
            self.retransmission_timer = None
            log.debug(f"Flow {self.port_id}: Retransmission timer cancelled.")

    def _handle_timeout(self):
        """Handles retransmission timer expiration."""
        self.retransmission_timer = None # Timer handle is used
        if self.last_pdu_sent and self.state in [FlowState.ESTABLISHED]: # Add other relevant states
            if self.retry_count < MAX_RETRIES:
                self.retry_count += 1
                log.warning(f"Flow {self.port_id}: Timeout! Retrying PDU Seq={self.last_pdu_sent.seq_num} (Attempt {self.retry_count})")
                # Using asyncio.create_task to run the async _send_pdu
                asyncio.create_task(self._send_pdu(self.last_pdu_sent))
            else:
                log.error(f"Flow {self.port_id}: Max retries exceeded for PDU Seq={self.last_pdu_sent.seq_num}. Closing flow.")
                # Handle connection failure - potentially close the flow abruptly
                asyncio.create_task(self.close(graceful=False))
        else:
             log.debug(f"Flow {self.port_id}: Timeout handled, but no PDU to retransmit or wrong state ({self.state}).")


    async def prepare_data_pdus(self, data: bytes) -> List[bytes]:
        """
        Prepares Data PDUs for sending. Implements simplified Stop-and-Wait.
        Returns list of serialized PDUs (bytes). In stop-and-wait, this is usually one PDU.
        """
        if self.state != FlowState.ESTABLISHED:
            log.warning(f"Flow {self.port_id}: Cannot send data, flow not established (State: {self.state}).")
            return []

        # Stop-and-Wait: Only send if the previous PDU has been ACKed
        if self.last_pdu_sent and self.last_ack_received < self.last_pdu_sent.seq_num:
             log.warning(f"Flow {self.port_id}: Cannot send new data yet, waiting for ACK of Seq={self.last_pdu_sent.seq_num}")
             # TODO: Implement buffering of outgoing data if needed
             return []

        # Simple case: one segment per PDU
        pdu = PDU(
            pdu_type=PduType.DATA,
            dest_port_id=self.peer_port_id,
            src_port_id=self.port_id,
            seq_num=self.send_seq_num,
            ack_num=self.receive_seq_num, # Piggyback ACK if needed
            flags=PduFlags.ACK_FLAG,     # Always include ACK num
            payload=data
        )
        self.send_seq_num += 1 # Increment for next time
        await self._send_pdu(pdu) # Send immediately and set timer

        # Stop-and-wait returns immediately after sending, doesn't wait for ACK here
        return [] # We don't return bytes here, sending is done in _send_pdu

    async def process_received_pdu(self, pdu_bytes: bytes):
        """Processes a received PDU according to protocol rules."""
        pdu = PDU.from_bytes(pdu_bytes)
        if not pdu:
            log.warning(f"Flow {self.port_id}: Discarding malformed PDU.")
            return

        log.debug(f"Flow {self.port_id}: Processing received PDU: {pdu}")

        # --- ACK Processing ---
        if pdu.flags & PduFlags.ACK_FLAG:
            ack_num = pdu.ack_num
            if ack_num > self.last_ack_received:
                log.debug(f"Flow {self.port_id}: Received ACK for Seq={ack_num}. Previous ACK was {self.last_ack_received}")
                # If this ACKs our outstanding PDU
                if self.last_pdu_sent and ack_num == self.last_pdu_sent.seq_num:
                     self._cancel_retransmission_timer()
                     self.last_ack_received = ack_num
                     self.last_pdu_sent = None # Ready to send next one
                     # TODO: If buffering outgoing data, trigger sending next segment
                # Handle cumulative ACKs if implementing sliding window later
            else:
                log.debug(f"Flow {self.port_id}: Received duplicate or old ACK ({ack_num}). Ignored.")


        # --- DATA Processing ---
        if pdu.pdu_type == PduType.DATA:
            # Check if it's the sequence number we expect
            if pdu.seq_num == self.receive_seq_num:
                log.debug(f"Flow {self.port_id}: Received expected DATA Seq={pdu.seq_num}")
                # Process payload
                if pdu.payload:
                    await self.local_ipcp.deliver_to_app(self.port_id, pdu.payload)
                # Update expected sequence number
                self.receive_seq_num += 1
                # Send ACK
                await self._send_ack()
            elif pdu.seq_num < self.receive_seq_num:
                log.warning(f"Flow {self.port_id}: Received duplicate DATA Seq={pdu.seq_num} (expected {self.receive_seq_num}). Sending ACK.")
                # Resend ACK for the highest sequence number received so far
                await self._send_ack()
            else:
                log.warning(f"Flow {self.port_id}: Received out-of-order DATA Seq={pdu.seq_num} (expected {self.receive_seq_num}). Discarding (or buffering).")
                # TODO: Implement buffering for out-of-order packets if needed
                # If buffering, still send ACK for the last in-order packet received
                await self._send_ack()

        # --- FIN Processing ---
        if pdu.flags & PduFlags.FIN_FLAG:
            log.info(f"Flow {self.port_id}: Received FIN flag from peer.")
            # TODO: Implement FIN state transitions (CLOSE_WAIT, etc.)
            # Acknowledge the FIN
            await self._send_ack()
            # Transition state, e.g., self.state = FlowState.CLOSE_WAIT
            # Application should be notified, potentially initiate closing from this side.
            # For simplicity, we might just transition to CLOSED after sending ACK.
            self.state = FlowState.CLOSED
            self._cancel_retransmission_timer() # Stop timers if closing

        # --- Other PDU Types (SYN, etc.) ---
        # TODO: Handle SYN if implementing explicit 3-way handshake


    async def _send_ack(self):
        """Sends an acknowledgment PDU."""
        ack_pdu = PDU(
            pdu_type=PduType.ACK,
            dest_port_id=self.peer_port_id,
            src_port_id=self.port_id,
            seq_num=self.send_seq_num, # Sequence number might not be relevant for pure ACK
            ack_num=self.receive_seq_num, # Acknowledge up to this sequence number
            flags=PduFlags.ACK_FLAG,
            payload=b''
        )
        # Use a separate task to avoid blocking current PDU processing
        asyncio.create_task(self._send_pdu(ack_pdu)) # ACK sending itself isn't timed/retried usually

    async def close(self, graceful=True):
        """Initiates flow termination."""
        log.info(f"Flow {self.port_id}: Initiating close (graceful={graceful}). Current state: {self.state}")
        if self.state in [FlowState.CLOSED, FlowState.FIN_WAIT_1, FlowState.FIN_WAIT_2, FlowState.CLOSING, FlowState.LAST_ACK, FlowState.TIME_WAIT]:
            log.warning(f"Flow {self.port_id}: Already closing or closed.")
            return

        self._cancel_retransmission_timer() # Stop any pending retransmissions

        if graceful:
            # Send FIN PDU
            fin_pdu = PDU(
                pdu_type=PduType.FIN, # Or DATA with FIN flag set
                dest_port_id=self.peer_port_id,
                src_port_id=self.port_id,
                seq_num=self.send_seq_num,
                ack_num=self.receive_seq_num, # Piggyback latest ACK
                flags=PduFlags.FIN_FLAG | PduFlags.ACK_FLAG,
                payload=b''
            )
            # TODO: Implement state transitions (FIN_WAIT_1, etc.) and wait for peer FIN/ACK
            await self._send_pdu(fin_pdu)
            self.state = FlowState.FIN_WAIT_1 # Simplified: Assume FIN sent starts waiting
        else:
            # Abrupt close
            self.state = FlowState.CLOSED
            # TODO: Optionally send an ABORT PDU type

        log.info(f"Flow {self.port_id}: Close initiated. New state: {self.state}")

    def __repr__(self) -> str:
         return (f"Flow(Port:{self.port_id}, Peer:'{self.peer_app_name}:{self.peer_port_id}', "
                 f"State:{self.state.name}, NextSendSeq:{self.send_seq_num}, NextRecvSeq:{self.receive_seq_num})")