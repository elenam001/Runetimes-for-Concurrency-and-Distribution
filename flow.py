import logging
import socket
import struct
import time
import protocol
import asyncio
import statistics
import os
import random

class Flow:
    def __init__(self, apn, host, port):
        self.apn = apn
        self.host = host
        self.port = port
        self.flow_id = None
        self.connected = False
        self.reader = None
        self.writer = None
        self.lock = asyncio.Lock()  # For thread-safe operations
        self.retry_base_delay = 0.1
        self.last_heartbeat = time.time()

    async def allocate(self, retries=5):
        if self.connected:
            return

        for attempt in range(retries):
            try:
                # Create connection with optimized socket options
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port,
                    ssl=None,
                    family=socket.AF_INET,
                    flags=socket.SOCK_STREAM,
                    proto=socket.IPPROTO_TCP,
                )
                
                # Set socket options for high performance
                sock = self.writer.get_extra_info('socket')
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2*1024*1024)  # 2MB send buffer
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2*1024*1024)  # 2MB receive buffer
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm

                # Send allocation request
                request = protocol.pack_message(
                    flow_id="FLOW_REQ",
                    payload=f"REQ:{self.apn}".encode()
                )
                self.writer.write(request)
                await self.writer.drain()

                # Read response with timeout
                response = await asyncio.wait_for(self.reader.read(65536), timeout=2.0)
                _, received_flow_id, _ = protocol.unpack_message(response)
                if ':' not in received_flow_id:
                    raise ValueError("Invalid flow ID format received from server")

                if not received_flow_id:
                    raise ValueError("No flow ID received from server")

                self.flow_id = received_flow_id
                self.connected = True
                logging.debug(f"Successfully allocated flow {self.flow_id}")
                return

            except Exception as e:
                logging.error(f"Allocation attempt {attempt+1} failed: {str(e)}")
                if self.writer:
                    self.writer.close()
                    await self.writer.wait_closed()
                await self._exponential_backoff(attempt)
        
        self.connected = False
        raise ConnectionError("Flow allocation failed after retries")

    async def teardown(self):
        async with self.lock:
            if not self.connected or not self.writer:
                return

            try:
                teardown_msg = protocol.pack_message(
                    flow_id=self.flow_id,
                    payload=f"TEARDOWN:{self.flow_id}".encode()
                )
                self.writer.write(teardown_msg)
                await asyncio.wait_for(self.writer.drain(), timeout=1.0)
                await asyncio.wait_for(self.reader.read(65536), timeout=1.0)  # Wait for ACK
            except Exception as e:
                logging.debug(f"Teardown error: {str(e)}")
            finally:
                self.writer.close()
                await self.writer.wait_closed()
                self.connected = False
                self.flow_id = None
                self.reader = None
                self.writer = None

    async def send(self, payload_size=1024, retries=3, parallel=4):
        async with self.lock:
            if not self.connected:
                await self.allocate()

            try:
                return await self._send_burst(payload_size, parallel, retries)
            except Exception as e:
                logging.error(f"Critical send error: {str(e)}")
                await self.teardown()
                return None

    async def _send_burst(self, payload_size, parallel_connections, max_retries):
        async def _send_single_new_connection():
            # Create a new Flow instance for each connection
            temp_flow = Flow(self.apn, self.host, self.port)
            try:
                await temp_flow.allocate()
                latency = await temp_flow._send_single(payload_size, max_retries)
                return latency
            finally:
                await temp_flow.teardown()
        
        tasks = [_send_single_new_connection() for _ in range(parallel_connections)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_latencies = [lat for lat in results if lat is not None and not isinstance(lat, Exception)]
        
        if valid_latencies:
            return statistics.mean(valid_latencies)
        return None

    async def _send_single(self, payload_size, max_retries):
        payload = os.urandom(payload_size)
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time()
                
                if not self.connected:
                    await self.allocate()
                
                # Send payload
                self.writer.write(protocol.pack_message(self.flow_id, payload))
                await asyncio.wait_for(self.writer.drain(), timeout=1.0)
                
                # Read ACK properly
                header_data = await asyncio.wait_for(self.reader.readexactly(protocol.HEADER_SIZE), timeout=1.0)
                timestamp, payload_size_ack = struct.unpack(protocol.HEADER_FORMAT, header_data)
                flow_id_data = await asyncio.wait_for(self.reader.readexactly(protocol.FLOW_ID_LENGTH), timeout=1.0)
                flow_id_ack = flow_id_data.decode().rstrip('\0')
                ack_payload = await asyncio.wait_for(self.reader.readexactly(payload_size_ack), timeout=1.0)
                
                if ack_payload != b"ACK":
                    raise ConnectionError("Invalid ACK received")

                return time.time() - start_time

            except (asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
                logging.debug(f"Send attempt {attempt+1} failed: {str(e)}")
                if attempt == max_retries:
                    return None
                
                await self._exponential_backoff(attempt)
                await self._reconnect_if_needed()
            except Exception as e:
                logging.debug(f"Send attempt {attempt+1} failed: {str(e)}")
                if attempt == max_retries:
                    return None
                await self._exponential_backoff(attempt)
                await self._reconnect_if_needed()

    async def _reconnect_if_needed(self):
        async with self.lock:
            if self.connected:
                try:
                    # Send heartbeat to check connection
                    self.writer.write(protocol.pack_message("HEARTBEAT", b""))
                    await asyncio.wait_for(self.writer.drain(), timeout=0.5)
                    return
                except:
                    pass

            await self.teardown()
            await self.allocate()

    async def _exponential_backoff(self, attempt):
        max_delay = 5.0
        base_delay = min(self.retry_base_delay * (2 ** attempt), max_delay)
        jitter = random.uniform(0, base_delay * 0.1)  # Add 10% jitter
        await asyncio.sleep(base_delay + jitter)