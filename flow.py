import logging
import socket
import time
import protocol
import asyncio
import statistics
import os

class Flow:
    def __init__(self, apn, host, port):
        self.apn = apn
        self.host = host
        self.port = port
        self.flow_id = None
        self.connected = False
        self.reuse_threshold = 20
        self.packets_sent = 0
        self._reader = None
        self._writer = None

    async def allocate(self, retries=5):
        for attempt in range(retries):
            try:
                self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
                self.connected = True
                # Send allocation request
                request = protocol.pack_message(
                    flow_id="FLOW_REQ",
                    payload=f"REQ:{self.apn}".encode()
                )
                self._writer.write(request)
                await self._writer.drain()

                # Read response
                response = await self._reader.read(65536)
                _, received_flow_id, _ = protocol.unpack_message(response)

                if not received_flow_id:
                    raise ValueError("No flow ID received from server.")

                self.flow_id = received_flow_id
                return

            except Exception as e:
                logging.error(f"Allocation attempt {attempt+1} failed: {str(e)}")
                await asyncio.sleep(2 ** attempt)

        self.connected = False
        raise Exception("Flow allocation failed after retries.")

    async def teardown(self):
        if not self.flow_id or not self._writer:
            return
        try:
            teardown_msg = protocol.pack_message(
                flow_id=self.flow_id,
                payload=f"TEARDOWN:{self.flow_id}".encode()
            )
            self._writer.write(teardown_msg)
            await self._writer.drain()
            ack = await self._reader.read(65536)
        except Exception as e:
            logging.error(f"Teardown error: {e}")
        finally:
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
            self._reader = None
            self._writer = None
            self.flow_id = None
            self.connected = False

    async def send(self, payload_size=1024, retries=2, parallel=4):
        if not self.connected:
            await self.allocate()
            if not self.connected:
                return None

        latencies = await self._async_send_data(payload_size, parallel)
        if latencies:
            self.packets_sent += parallel
            return statistics.mean(latencies)
        return None

    async def _async_send_data(self, payload_size, parallel_connections):
        latencies = []
        for _ in range(parallel_connections):
            try:
                payload = os.urandom(payload_size)
                start_time = time.time()
                self._writer.write(protocol.pack_message(self.flow_id, payload))
                await self._writer.drain()
                ack = await self._reader.read(65536)
                latency = time.time() - start_time
                latencies.append(latency)
            except Exception as e:
                logging.error(f"Error sending data: {e}")
        return latencies

    async def _send_heartbeat(self):
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            writer.write(protocol.pack_message(
                flow_id="HEARTBEAT",
                payload=b""
            ))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            logging.debug(f"Heartbeat failed: {str(e)}")