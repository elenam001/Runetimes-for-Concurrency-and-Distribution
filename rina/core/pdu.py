import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

from .naming import PortId

# Define constants for PDU structure
# Using simple struct format strings
# ! = network byte order (big-endian)
HEADER_FORMAT = "!BIIIIB" # TYPE, DST_PORT, SRC_PORT, SEQ, ACK, FLAGS
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# --- PDU Types ---
class PduType(IntEnum):
    DATA = 1
    ACK = 2
    SYN = 3  # For flow setup (optional, can combine with ACK)
    FIN = 4  # For flow termination
    # Add other types as needed (e.g., enrollment, flow_alloc management PDUs)

# --- PDU Flags ---
# Use powers of 2 for bitwise operations
class PduFlags(IntEnum):
    NONE = 0
    ACK_FLAG = 1  # Indicates the ACK number is valid
    SYN_FLAG = 2  # Synchronize sequence numbers (part of handshake)
    FIN_FLAG = 4  # Finish, no more data from sender

@dataclass
class PDU:
    """Base class/structure for Protocol Data Units."""
    pdu_type: PduType
    dest_port_id: PortId
    src_port_id: PortId
    seq_num: int = 0          # Sequence number
    ack_num: int = 0          # Acknowledgment number (if ACK_FLAG is set)
    flags: PduFlags = PduFlags.NONE
    payload: bytes = field(default_factory=bytes)

    def to_bytes(self) -> bytes:
        """Serializes the PDU into bytes."""
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

    @classmethod
    def from_bytes(cls, raw_pdu: bytes) -> Optional['PDU']:
        """Deserializes bytes into a PDU object."""
        if len(raw_pdu) < HEADER_SIZE:
            # log.warning("Received PDU shorter than minimum header size.") # Add logging
            return None

        header_bytes = raw_pdu[:HEADER_SIZE]
        payload_bytes = raw_pdu[HEADER_SIZE:]

        try:
            pdu_type_val, dest_port, src_port, seq, ack, flags_val = struct.unpack(HEADER_FORMAT, header_bytes)
            pdu_type = PduType(pdu_type_val)
            flags = PduFlags(flags_val)
        except (struct.error, ValueError) as e:
            # log.error(f"Failed to unpack or interpret PDU header: {e}") # Add logging
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
        flag_str = "|".join(f.name for f in PduFlags if self.flags & f and f != PduFlags.NONE) or "NONE"
        payload_str = f"{len(self.payload)} bytes" if self.payload else "no payload"
        return (f"PDU(Type:{self.pdu_type.name}, Dst:{self.dest_port_id}, Src:{self.src_port_id}, "
                f"Seq:{self.seq_num}, Ack:{self.ack_num}, Flags:{flag_str}, Payload:{payload_str})")