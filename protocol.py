import logging
import struct
import time

# Define binary header format (8-byte timestamp + 4-byte payload size)
HEADER_FORMAT = '!dI'  # 8B timestamp + 4B payload size (Big-Endian)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
FLOW_ID_LENGTH = 32  # Fixed-length flow ID

def pack_message(flow_id: str, payload: bytes) -> bytes:
    """
    Pack message into binary format with a fixed-size header.

    Format:
    - 8B Timestamp (double)
    - 4B Payload Size (unsigned int)
    - 32B Flow ID (null-padded)
    - Variable-length Payload
    """
    # Ensure flow_id is exactly FLOW_ID_LENGTH bytes (null-padded)
    flow_bytes = flow_id.encode().ljust(FLOW_ID_LENGTH, b'\0')[:FLOW_ID_LENGTH]

    # Create the header
    header = struct.pack(HEADER_FORMAT, time.time(), len(payload))

    # Return the full packed message
    return header + flow_bytes + payload

def unpack_message(data: bytes) -> tuple:
    """
    Unpack a binary message.

    Returns:
    - timestamp (float)
    - flow_id (str)
    - payload (bytes)
    
    If the message is incomplete or corrupted, returns (None, None, None).
    """
    # Ensure we have at least enough data for the header + flow_id
    if len(data) < HEADER_SIZE + FLOW_ID_LENGTH:
        logging.error(f"Received incomplete message: {len(data)} bytes")
        return None, None, None

    try:
        # Extract timestamp and payload size
        timestamp, payload_size = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        
        # Extract flow ID (removing null-padding)
        flow_id = data[HEADER_SIZE:HEADER_SIZE+FLOW_ID_LENGTH].decode().rstrip('\0')

        # Ensure full payload is received
        total_size = HEADER_SIZE + FLOW_ID_LENGTH + payload_size
        if len(data) < total_size:
            logging.error(f"Received truncated payload: expected {total_size}, got {len(data)}")
            return None, None, None

        # Extract payload
        payload = data[HEADER_SIZE+FLOW_ID_LENGTH:total_size]
        return timestamp, flow_id, payload

    except struct.error as e:
        logging.error(f"Corrupted binary header: {str(e)}")
    except Exception as e:
        logging.error(f"Unpack error: {str(e)}")

    return None, None, None
