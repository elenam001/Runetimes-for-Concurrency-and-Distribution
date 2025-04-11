import struct
import logging # Added logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

from .naming import PortId

# Setup basic logging
log = logging.getLogger(__name__)

# Define constants for PDU structure
# Using simple struct format strings
# ! = network byte order (big-endian)
HEADER_FORMAT = "!BIIIIB" # TYPE, DST_PORT, SRC_PORT, SEQ, ACK, FLAGS
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# --- PDU Types ---
class PduType(IntEnum):
    # Data Transfer Protocol Types
    DATA = 1
    ACK = 2
    # SYN/FIN are often combined with DATA/ACK via flags in TCP, but can be distinct
    # Keep distinct types for potential explicit use
    SYN = 3  # For flow setup handshake (optional)
    FIN = 4  # For flow termination handshake

    # Management Protocol Types
    # Using a range distinct from data transfer
    MGMT_ENROLL_REQ = 10    # Enrollment Request
    MGMT_ENROLL_RESP = 11   # Enrollment Response
    MGMT_FLOW_ALLOC_REQ = 12 # Flow Allocation Request
    MGMT_FLOW_ALLOC_RESP = 13 # Flow Allocation Response
    MGMT_FLOW_DEALLOC = 14   # Flow Deallocation Notification/Request (optional)
    # Add other management types (e.g., Directory lookup, Policy update) if needed


# --- PDU Flags ---
# Use powers of 2 for bitwise operations
class PduFlags(IntEnum):
    NONE = 0
    # Data Transfer Flags (often used with DATA/ACK types)
    ACK_FLAG = 1  # Indicates the ACK number field is valid
    SYN_FLAG = 2  # Synchronize sequence numbers (flow setup handshake)
    FIN_FLAG = 4  # Finish, no more data from sender (flow termination)
    # RST_FLAG = 8 # Reset the connection (optional)

    # Management Flags (could be used with MGMT types, if needed)
    # e.g., MGMT_SUCCESS = 16 # Indicates success in a response (alternative to payload field)


@dataclass
class PDU:
    """Base class/structure for Protocol Data Units."""
    pdu_type: PduType
    # For Data Transfer PDUs (DATA, ACK, SYN, FIN), ports identify flow endpoints.
    # For Management PDUs (MGMT_*), ports might be 0 or identify specific mgmt instances/flows.
    dest_port_id: PortId
    src_port_id: PortId
    # Sequence/Ack numbers are primarily for reliable data transfer protocols.
    # They might be unused or repurposed (e.g., as Transaction IDs) for Management PDUs,
    # but keeping the header fixed means they exist. For now, assume they are 0 for MGMT PDUs
    # unless explicitly used by a management protocol.
    seq_num: int = 0
    ack_num: int = 0
    flags: PduFlags = PduFlags.NONE
    payload: bytes = field(default_factory=bytes) # Data or Serialized Management Info (e.g., JSON)
    source_address: Optional[str] = None # Optional source address for routing (e.g., IP address)

    def to_bytes(self) -> bytes:
        """Serializes the PDU into bytes."""
        try:
            header = struct.pack(
                HEADER_FORMAT,
                self.pdu_type.value,
                self.dest_port_id,
                self.src_port_id,
                self.seq_num,
                self.ack_num,
                self.flags.value
        )
            return header + self.payload
        except struct.error as e:
            log.error(f"Failed to pack PDU header: {e}. PDU data: {self}", exc_info=True)
            return b"" # Return empty bytes on failure

    @classmethod
    def from_bytes(cls, raw_pdu: bytes) -> Optional['PDU']:
        """Deserializes bytes into a PDU object."""
        if len(raw_pdu) < HEADER_SIZE:
            log.warning(f"Received PDU shorter than minimum header size ({len(raw_pdu)} < {HEADER_SIZE}).")
            return None

        header_bytes = raw_pdu[:HEADER_SIZE]
        payload_bytes = raw_pdu[HEADER_SIZE:]

        try:
            pdu_type_val, dest_port, src_port, seq, ack, flags_val = struct.unpack(HEADER_FORMAT, header_bytes)
            # Validate PduType - raises ValueError if invalid
            pdu_type = PduType(pdu_type_val)
            # Validate PduFlags (allow any combination of defined bits)
            # Check if flags_val contains undefined bits (optional strictness)
            # known_flags_mask = sum(f.value for f in PduFlags) # Calculate mask of all known flags
            # if flags_val & ~known_flags_mask:
            #    log.warning(f"PDU received with unknown flags set: {flags_val & ~known_flags_mask}")
            flags = PduFlags(flags_val) # Still store the value

        except ValueError:
            # Invalid PDU type value
            log.error(f"Failed to interpret PDU header: Invalid PDU Type value {pdu_type_val}.")
            return None
        except struct.error as e:
            # Other unpacking errors
            log.error(f"Failed to unpack PDU header: {e}.")
            return None

        return cls(
            pdu_type=pdu_type,
            dest_port_id=PortId(dest_port),
            src_port_id=PortId(src_port),
            seq_num=seq,
            ack_num=ack,
            flags=flags,
            payload=payload_bytes
        )

    def __repr__(self):
        try:
            type_name = self.pdu_type.name
        except ValueError:
             type_name = f"UNKNOWN({self.pdu_type})" # Handle case if type is somehow invalid int

        # Generate string for known flags that are set
        set_flags = [f.name for f in PduFlags if f != PduFlags.NONE and (self.flags & f)]
        flag_str = "|".join(set_flags) if set_flags else "NONE"

        payload_str = f"{len(self.payload)} bytes" if self.payload else "no payload"
        # Optionally try to decode payload if known type (e.g., JSON for MGMT) for repr
        # Be careful with large payloads in repr
        # if self.pdu_type >= PduType.MGMT_ENROLL_REQ and self.payload:
        #    try: payload_str = self.payload.decode('utf-8', errors='replace')
        #    except: pass # Ignore decode errors for repr

        return (f"PDU(Type:{type_name}, Dst:{self.dest_port_id}, Src:{self.src_port_id}, "
                f"Seq:{self.seq_num}, Ack:{self.ack_num}, Flags:{flag_str}, Payload:{payload_str})")