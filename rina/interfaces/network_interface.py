import abc
import asyncio
import logging
from typing import Any, Callable, Dict, Tuple, Optional

log = logging.getLogger(__name__)

# Define type alias for clarity
# Handler takes: raw received data (bytes), source address (implementation-specific format)
# Handler should be an async function or coroutine
PacketHandler = Callable[[bytes, Any], asyncio.Future]
UnderlyingAddress = Any # Type alias for addresses used by the specific interface

# --- Abstract Base Class ---

class NetworkInterface(abc.ABC):
    """
    Abstract base class for low-level network interfaces (DIF 0).

    Defines the contract for sending data and registering handlers
    for received data on specific local endpoints managed by this interface.
    """

    @abc.abstractmethod
    async def start(self):
        """Initializes and starts the network interface."""
        pass

    @abc.abstractmethod
    async def stop(self):
        """Stops the network interface and releases resources."""
        pass

    @abc.abstractmethod
    async def send(self, data: bytes, destination_address: UnderlyingAddress) -> bool:
        """
        Sends data to the specified destination address.

        Args:
            data: The raw bytes to send.
            destination_address: The address format depends on the implementation
                                 (e.g., (ip, port) for UDP).

        Returns:
            True if sending was initiated successfully, False otherwise.
        """
        pass

    @abc.abstractmethod
    def register_handler(self, local_address: UnderlyingAddress, handler: PacketHandler):
        """
        Registers a handler function to process packets received for a specific local address
        managed by this interface.

        Args:
            local_address: The specific local address (e.g., (ip, port)) this handler listens on.
                           Must be consistent with the interface's configuration.
            handler: An async callable that accepts (data: bytes, source_address: Any).
        """
        pass

    @abc.abstractmethod
    def unregister_handler(self, local_address: UnderlyingAddress):
        """Unregisters the handler for a specific local address."""
        pass

# --- UDP Implementation ---

class _UdpProtocol(asyncio.DatagramProtocol):
    """Asyncio Protocol implementation helper for UdpInterface."""
    def __init__(self, interface: 'UdpInterface'):
        self.interface = interface
        self.transport: Optional[asyncio.DatagramTransport] = None
        super().__init__()

    def connection_made(self, transport: asyncio.DatagramTransport):
        """Called when the socket is set up."""
        self.transport = transport
        sockname = transport.get_extra_info('sockname')
        log.info(f"UDP Interface {self.interface.interface_id}: Socket opened and listening on {sockname}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Called when a datagram is received."""
        log.debug(f"UDP Interface {self.interface.interface_id}: Received {len(data)} bytes from {addr}")
        # Determine the intended local address (usually the bind address of the interface)
        local_addr = self.interface.local_address
        # Find the registered handler for this interface's local address
        handler = self.interface.handlers.get(local_addr)

        if handler:
            # Schedule the handler execution as a separate task
            asyncio.create_task(handler(data, addr))
        else:
            log.warning(f"UDP Interface {self.interface.interface_id}: No handler registered for local address {local_addr}. Discarding packet from {addr}.")

    def error_received(self, exc: Exception):
        """Called when a send or receive operation raises an OSError."""
        # Log OS errors (e.g., ICMP port unreachable)
        log.error(f"UDP Interface {self.interface.interface_id}: Error received: {exc}")
        # Some errors might indicate the destination is unreachable.
        # We might want to signal this upwards, but UDP is connectionless.

    def connection_lost(self, exc: Optional[Exception]):
        """Called when the transport is closed."""
        log.info(f"UDP Interface {self.interface.interface_id}: Socket closed.")
        self.transport = None
        # Potentially signal interface closure or attempt reconnect if desired


class UdpInterface(NetworkInterface):
    """
    A NetworkInterface implementation using UDP datagrams via asyncio.
    Manages a single UDP socket bound to a specific local address/port.
    """

    def __init__(self, interface_id: str, local_ip: str, local_port: int):
        self.interface_id = interface_id
        self.local_address: Tuple[str, int] = (local_ip, local_port)
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UdpProtocol] = None
        # Handlers registered via register_handler (Key: local_address tuple)
        self.handlers: Dict[Tuple[str, int], PacketHandler] = {}
        self._loop = asyncio.get_running_loop()
        self._is_running = False
        log.info(f"UDP Interface '{self.interface_id}' initialized for {self.local_address}.")

    async def start(self):
        """Creates and starts the UDP datagram endpoint."""
        if self._is_running:
            log.warning(f"UDP Interface '{self.interface_id}' already running.")
            return

        log.info(f"UDP Interface '{self.interface_id}': Starting...")
        try:
            # Create the datagram endpoint
            transport, protocol = await self._loop.create_datagram_endpoint(
                lambda: _UdpProtocol(self), # Factory creates protocol instance
                local_addr=self.local_address
            )
            self._transport = transport
            self._protocol = protocol # Store protocol instance if needed later
            self._is_running = True
            log.info(f"UDP Interface '{self.interface_id}' started successfully.")
        except OSError as e:
            log.error(f"UDP Interface '{self.interface_id}': Failed to bind to {self.local_address}. Error: {e}", exc_info=True)
            self._transport = None
            self._protocol = None
            self._is_running = False
            raise # Re-raise exception to signal failure to start
        except Exception as e:
            log.error(f"UDP Interface '{self.interface_id}': Unexpected error during start: {e}", exc_info=True)
            self._is_running = False
            raise

    async def stop(self):
        """Closes the UDP transport."""
        if not self._is_running or not self._transport:
            log.warning(f"UDP Interface '{self.interface_id}' is not running or transport is missing.")
            return

        log.info(f"UDP Interface '{self.interface_id}': Stopping...")
        try:
             self._transport.close()
        except Exception as e:
             log.error(f"UDP Interface '{self.interface_id}': Error closing transport: {e}", exc_info=True)
        finally:
             self._transport = None
             self._protocol = None
             self._is_running = False
             # Allow time for connection_lost to be called if protocol needs cleanup
             await asyncio.sleep(0.01)
             log.info(f"UDP Interface '{self.interface_id}' stopped.")

    async def send(self, data: bytes, destination_address: Tuple[str, int]) -> bool:
        """Sends data using the UDP transport."""
        if not self._is_running or not self._transport:
            log.error(f"UDP Interface '{self.interface_id}': Cannot send, not running or transport unavailable.")
            return False

        if not isinstance(destination_address, tuple) or len(destination_address) != 2:
             log.error(f"UDP Interface '{self.interface_id}': Invalid destination address format for UDP: {destination_address}. Expected (ip_str, port_int).")
             return False

        try:
            log.debug(f"UDP Interface '{self.interface_id}': Sending {len(data)} bytes to {destination_address}")
            self._transport.sendto(data, destination_address)
            # Note: UDP sendto is generally non-blocking and doesn't guarantee delivery.
            # We assume success if no immediate exception occurs.
            return True
        except OSError as e:
            # E.g., Network unreachable, buffer full (less common for UDP)
            log.error(f"UDP Interface '{self.interface_id}': OS Error sending to {destination_address}: {e}")
            return False
        except Exception as e:
            log.error(f"UDP Interface '{self.interface_id}': Unexpected error sending to {destination_address}: {e}", exc_info=True)
            return False

    def register_handler(self, local_address: Tuple[str, int], handler: PacketHandler):
        """Registers a handler for packets received on the interface's bind address."""
        # This simple UDP interface only manages one local address (its bind address).
        if local_address != self.local_address:
            log.error(f"UDP Interface '{self.interface_id}': Cannot register handler for {local_address}. Interface only handles {self.local_address}.")
            return

        if not asyncio.iscoroutinefunction(handler):
             log.error(f"UDP Interface '{self.interface_id}': Handler must be an async function (using 'async def').")
             return

        log.info(f"UDP Interface '{self.interface_id}': Registering handler for {local_address}.")
        self.handlers[local_address] = handler

    def unregister_handler(self, local_address: Tuple[str, int]):
        """Unregisters the handler for the interface's bind address."""
        if local_address != self.local_address:
            log.warning(f"UDP Interface '{self.interface_id}': Cannot unregister handler for {local_address}. Interface only handles {self.local_address}.")
            return

        removed_handler = self.handlers.pop(local_address, None)
        if removed_handler:
            log.info(f"UDP Interface '{self.interface_id}': Unregistered handler for {local_address}.")
        else:
            log.warning(f"UDP Interface '{self.interface_id}': No handler was registered for {local_address} to unregister.")

    def __repr__(self) -> str:
         status = "Running" if self._is_running else "Stopped"
         return f"UdpInterface(id='{self.interface_id}', address={self.local_address}, status={status})"