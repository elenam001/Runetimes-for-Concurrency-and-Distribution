# protocol.py
import logging
import struct
import time

HEADER_FORMAT = '!dI'  # 8B timestamp + 4B payload size
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
FLOW_ID_LENGTH = 32

def pack_message(flow_id: str, payload: bytes) -> bytes:
    """Pack message with header, flow ID, and payload"""
    flow_bytes = flow_id.encode().ljust(FLOW_ID_LENGTH, b'\0')[:FLOW_ID_LENGTH]
    header = struct.pack(HEADER_FORMAT, time.time(), len(payload))
    return header + flow_bytes + payload

def unpack_message(data: bytes) -> tuple:
    try:
        if len(data) < HEADER_SIZE + FLOW_ID_LENGTH:
            # Instead of raising, return None to indicate incomplete message
            return None, None, None
        timestamp, payload_size = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        flow_id = data[HEADER_SIZE:HEADER_SIZE+FLOW_ID_LENGTH].decode().rstrip('\0')
        payload = data[HEADER_SIZE+FLOW_ID_LENGTH:HEADER_SIZE+FLOW_ID_LENGTH+payload_size]
        return timestamp, flow_id, payload
    except struct.error as e:
        logging.error(f"Corrupted header: {str(e)}")
        return None, None, None
    except Exception as e:
        logging.error(f"Unpack error: {str(e)}")
        return None, None, None
    