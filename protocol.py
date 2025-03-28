import logging
import struct
import time

# Define binary header format (8-byte timestamp + 4-byte payload size)
HEADER_FORMAT = '!dI'  # Keep original format with timestamp and size
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # Should be 12 bytes (8 + 4)
FLOW_ID_LENGTH = 32

def pack_message(flow_id: str, payload: bytes) -> bytes:
    """Pack message with timestamp and payload size"""
    flow_bytes = flow_id.encode().ljust(FLOW_ID_LENGTH, b'\0')[:FLOW_ID_LENGTH]
    header = struct.pack(
        HEADER_FORMAT, 
        time.time(),    # 8B timestamp (double)
        len(payload)    # 4B payload size (unsigned int)
    )
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
