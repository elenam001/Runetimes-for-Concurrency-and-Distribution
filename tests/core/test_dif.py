import unittest
from unittest.mock import MagicMock, AsyncMock, patch, call
import asyncio
import json

# --- RINA imports (adjust paths if necessary based on your project structure) ---
# Assume these imports work relative to the test execution context
from rina.core.dif import DIF
from rina.core.pdu import PDU, PduType, PduFlags
from rina.core.naming import ApplicationName, PortId, DIFName
# We'll mock IPCProcess and LayerManager, but need their types for hints/checks
from rina.core.ipcp import IPCProcess
from rina.management.layer_manager import LayerManager
from rina.interfaces.network_interface import UnderlyingAddress, NetworkInterface

# Define dummy underlying address type for tests if needed
DummyUnderlyingAddress = tuple[str, int]

# --- Test Class ---

class TestDIF(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up common resources for DIF tests."""
        self.dif_name = DIFName("test_dif")
        self.mock_layer_manager = AsyncMock(spec=LayerManager)
        # Mock the address resolution methods used by DIF
        self.mock_layer_manager.resolve_app_to_underlying_address = AsyncMock(return_value=("127.0.0.1", 9001))

        # Mock optional PolicyManager
        self.mock_policy_manager = AsyncMock() # spec=PolicyManager if defined

        self.dif = DIF(
            dif_name=self.dif_name,
            layer_manager=self.mock_layer_manager,
            policy_manager=self.mock_policy_manager,
            config={}
        )

        # Mock a transport mechanism for the DIF to send management PDUs
        # Let's assume it uses an IPCProcess as transport
        self.mock_mgmt_transport_ipcp = AsyncMock(spec=IPCProcess)
        self.mock_mgmt_transport_ipcp.app_name = ApplicationName("mgmt_ipcp")
        # Mock the sending method of the transport
        self.mock_mgmt_transport_ipcp._send_on_underlying = AsyncMock(return_value=True)
        self.dif.management_transport = self.mock_mgmt_transport_ipcp

        # Mock IPCProcess instances for registration/lookup tests
        self.mock_ipcp1 = AsyncMock(spec=IPCProcess)
        self.mock_ipcp1.app_name = ApplicationName("App1")
        self.mock_ipcp1.parent_dif = self.dif # Associate with the DIF
        self.mock_ipcp2 = AsyncMock(spec=IPCProcess)
        self.mock_ipcp2.app_name = ApplicationName("App2")
        self.mock_ipcp2.parent_dif = self.dif

        # Define a sample source address for incoming PDUs
        self.source_addr: DummyUnderlyingAddress = ("192.168.1.100", 5000)

    async def test_initialization(self):
        """Test basic DIF initialization."""
        self.assertEqual(self.dif.dif_name, self.dif_name)
        self.assertEqual(self.dif.layer_manager, self.mock_layer_manager)
        self.assertEqual(self.dif.policy_manager, self.mock_policy_manager)
        self.assertIsNotNone(self.dif._ipcps_lock)
        self.assertIsNotNone(self.dif._port_lock)
        self.assertEqual(len(self.dif._ipc_processes), 0)
        self.assertEqual(len(self.dif._allocated_ports), 0)

    async def test_ipcp_registration_and_lookup(self):
        """Test registering, unregistering, and resolving IPCPs."""
        app1_name = ApplicationName("App1")
        app2_name = ApplicationName("App2")

        # Register
        await self.dif.register_ipcp(app1_name, self.mock_ipcp1)
        self.assertEqual(len(self.dif._ipc_processes), 1)
        self.assertIn(str(app1_name), self.dif._ipc_processes)
        self.assertEqual(self.dif._ipc_processes[str(app1_name)], self.mock_ipcp1)

        # Resolve existing
        resolved_ipcp = await self.dif.resolve_name(app1_name)
        self.assertEqual(resolved_ipcp, self.mock_ipcp1)

        # Resolve non-existent
        resolved_ipcp_none = await self.dif.resolve_name(app2_name)
        self.assertIsNone(resolved_ipcp_none)

        # Unregister
        removed_ipcp = await self.dif.unregister_ipcp(app1_name)
        self.assertEqual(removed_ipcp, self.mock_ipcp1)
        self.assertEqual(len(self.dif._ipc_processes), 0)

        # Unregister non-existent
        removed_ipcp_none = await self.dif.unregister_ipcp(app2_name)
        self.assertIsNone(removed_ipcp_none)

    async def test_port_allocation(self):
        """Test allocation and release of Port IDs."""
        port1 = await self.dif._allocate_port_id()
        self.assertIsNotNone(port1)
        self.assertIsInstance(port1, int)
        self.assertGreaterEqual(port1, 10000) # Default start
        self.assertIn(port1, self.dif._allocated_ports)

        port2 = await self.dif._allocate_port_id()
        self.assertIsNotNone(port2)
        self.assertNotEqual(port1, port2)
        self.assertIn(port2, self.dif._allocated_ports)

        await self.dif._release_port_id(port1)
        self.assertNotIn(port1, self.dif._allocated_ports)
        self.assertIn(port2, self.dif._allocated_ports)

        # Test releasing non-allocated port (should not raise error)
        await self.dif._release_port_id(PortId(9999))

    async def test_send_management_pdu_success(self):
        """Test sending a management PDU via the configured transport."""
        response_pdu = PDU(pdu_type=PduType.MGMT_ENROLL_RESP, dest_port_id=PortId(0), src_port_id=PortId(0), payload=b'{}')
        dest_addr: DummyUnderlyingAddress = ("192.168.1.200", 6000)

        success = await self.dif._send_management_pdu(response_pdu, dest_addr)

        self.assertTrue(success)
        # Verify the transport's send method was called correctly
        self.mock_mgmt_transport_ipcp._send_on_underlying.assert_awaited_once_with(response_pdu, dest_addr)

    async def test_send_management_pdu_no_transport(self):
        """Test sending when no management transport is configured."""
        self.dif.management_transport = None # Remove transport
        response_pdu = PDU(pdu_type=PduType.MGMT_ENROLL_RESP, dest_port_id=PortId(0), src_port_id=PortId(0), payload=b'{}')
        dest_addr: DummyUnderlyingAddress = ("192.168.1.200", 6000)

        success = await self.dif._send_management_pdu(response_pdu, dest_addr)
        self.assertFalse(success)
        self.mock_mgmt_transport_ipcp._send_on_underlying.assert_not_called() # Check it wasn't called

    @patch('rina.core.dif.asyncio.create_task') # Mock task creation
    async def test_receive_management_pdu_dispatch(self, mock_create_task):
        """Test that receive_management_pdu dispatches to the correct handler."""
        # Mock handlers
        self.dif._handle_enrollment_request_pdu = AsyncMock()
        self.dif._handle_flow_request_pdu = AsyncMock()

        enroll_pdu = PDU(pdu_type=PduType.MGMT_ENROLL_REQ, dest_port_id=0, src_port_id=0, payload=b'{"tid": 1}')
        flow_pdu = PDU(pdu_type=PduType.MGMT_FLOW_ALLOC_REQ, dest_port_id=0, src_port_id=0, payload=b'{"tid": 2}')
        unknown_pdu = PDU(pdu_type=PduType.DATA, dest_port_id=0, src_port_id=0, payload=b'{}') # Non-mgmt type

        # Test enrollment dispatch
        await self.dif.receive_management_pdu(enroll_pdu, self.source_addr)
        mock_create_task.assert_called_once()
        # Get the coroutine passed to create_task
        handler_coro_enroll = mock_create_task.call_args[0][0]
        # Execute the coroutine to check if the right handler was called indirectly
        await handler_coro_enroll
        self.dif._handle_enrollment_request_pdu.assert_awaited_once_with(enroll_pdu, self.source_addr)
        self.dif._handle_flow_request_pdu.assert_not_called()
        mock_create_task.reset_mock() # Reset for next call
        self.dif._handle_enrollment_request_pdu.reset_mock()

        # Test flow dispatch
        await self.dif.receive_management_pdu(flow_pdu, self.source_addr)
        mock_create_task.assert_called_once()
        handler_coro_flow = mock_create_task.call_args[0][0]
        await handler_coro_flow
        self.dif._handle_flow_request_pdu.assert_awaited_once_with(flow_pdu, self.source_addr)
        self.dif._handle_enrollment_request_pdu.assert_not_called()
        mock_create_task.reset_mock()
        self.dif._handle_flow_request_pdu.reset_mock()

        # Test unknown dispatch (should log warning, not call handlers)
        await self.dif.receive_management_pdu(unknown_pdu, self.source_addr)
        mock_create_task.assert_not_called() # No handler, no task
        self.dif._handle_enrollment_request_pdu.assert_not_called()
        self.dif._handle_flow_request_pdu.assert_not_called()

    async def test_handle_enrollment_request_success(self):
        """Test successful enrollment request handling."""
        app_name = "EnrollApp"
        tid = 123
        request_payload = json.dumps({"app_name": app_name, "tid": tid}).encode('utf-8')
        request_pdu = PDU(
            pdu_type=PduType.MGMT_ENROLL_REQ,
            dest_port_id=PortId(0),
            src_port_id=PortId(500), # Example source port
            payload=request_payload
        )

        # Assume policy allows
        # self.mock_policy_manager.check_enrollment = AsyncMock(return_value=True) # If policy manager is strictly used

        # Mock the sending part
        self.dif._send_management_pdu = AsyncMock(return_value=True)

        await self.dif._handle_enrollment_request_pdu(request_pdu, self.source_addr)

        # Check that send_management_pdu was called
        self.dif._send_management_pdu.assert_awaited_once()
        # Get the arguments passed to send_management_pdu
        sent_pdu, dest_addr = self.dif._send_management_pdu.call_args[0]

        self.assertEqual(dest_addr, self.source_addr) # Should reply to source
        self.assertEqual(sent_pdu.pdu_type, PduType.MGMT_ENROLL_RESP)
        self.assertEqual(sent_pdu.dest_port_id, request_pdu.src_port_id) # Reply to original src port

        # Check payload contents
        response_data = json.loads(sent_pdu.payload.decode('utf-8'))
        self.assertEqual(response_data["status"], "success")
        self.assertEqual(response_data["tid"], tid)
        self.assertIn("assigned_address", response_data)
        self.assertIn("assigned_dif_name", response_data)
        self.assertEqual(response_data["assigned_dif_name"], self.dif_name)

    async def test_handle_enrollment_request_invalid_payload(self):
        """Test enrollment handling with invalid JSON payload."""
        request_pdu = PDU(
            pdu_type=PduType.MGMT_ENROLL_REQ,
            dest_port_id=PortId(0), src_port_id=PortId(501),
            payload=b'{"invalid json' # Malformed JSON
        )
        self.dif._send_management_pdu = AsyncMock(return_value=True)

        await self.dif._handle_enrollment_request_pdu(request_pdu, self.source_addr)

        self.dif._send_management_pdu.assert_awaited_once()
        sent_pdu, _ = self.dif._send_management_pdu.call_args[0]
        response_data = json.loads(sent_pdu.payload.decode('utf-8'))
        self.assertEqual(response_data["status"], "failure")
        self.assertIn("Invalid enrollment request PDU payload", response_data["reason"])
        self.assertNotIn("tid", response_data) # TID couldn't be parsed

    # --- Flow Allocation Tests ---
    # These are more complex due to IPCP lookups, port allocation, and notifications

    async def test_handle_flow_request_success(self):
        """Test successful flow allocation request handling."""
        source_app = ApplicationName("App1")
        target_app = ApplicationName("App2")
        tid = 456
        request_payload = json.dumps({
            "source_app_name": str(source_app),
            "target_app_name": str(target_app),
            "qos_params": {"reliability": "true"},
            "tid": tid
        }).encode('utf-8')
        request_pdu = PDU(
            pdu_type=PduType.MGMT_FLOW_ALLOC_REQ,
            dest_port_id=PortId(0), src_port_id=PortId(600),
            payload=request_payload
        )

        # Mock IPCP resolution
        self.dif.resolve_name = AsyncMock(side_effect=lambda name: self.mock_ipcp1 if name == source_app else (self.mock_ipcp2 if name == target_app else None))
        # Mock port allocation
        allocated_ports = [PortId(10001), PortId(10002)]
        self.dif._allocate_port_id = AsyncMock(side_effect=allocated_ports)
        # Mock sending response and notification
        self.dif._send_management_pdu = AsyncMock(return_value=True)
        # Mock resolving target underlying address for notification
        target_underlying_address = ("10.0.0.1", 9999)
        self.mock_layer_manager.resolve_app_to_underlying_address = AsyncMock(return_value=target_underlying_address)

        # Assume PDU Type for notification exists (add to pdu.py enum if missing)
        # Use a known existing type like MGMT_FLOW_ALLOC_RESP for testing if needed,
        # but ideally, define MGMT_FLOW_SETUP_NOTIFY.
        if not hasattr(PduType, 'MGMT_FLOW_SETUP_NOTIFY'):
            PduType.MGMT_FLOW_SETUP_NOTIFY = PduType.MGMT_FLOW_ALLOC_RESP # Temporary workaround for test

        await self.dif._handle_flow_request_pdu(request_pdu, self.source_addr)

        # 1. Check response sent to source
        self.assertEqual(self.dif._send_management_pdu.call_count, 2) # Response + Notification
        response_call = self.dif._send_management_pdu.call_args_list[0]
        resp_pdu, resp_addr = response_call[0]

        self.assertEqual(resp_addr, self.source_addr)
        self.assertEqual(resp_pdu.pdu_type, PduType.MGMT_FLOW_ALLOC_RESP)
        self.assertEqual(resp_pdu.dest_port_id, request_pdu.src_port_id)
        resp_data = json.loads(resp_pdu.payload.decode('utf-8'))
        self.assertEqual(resp_data["status"], "success")
        self.assertEqual(resp_data["tid"], tid)
        self.assertEqual(resp_data["flow_info"]["source_port_id"], allocated_ports[0])
        self.assertEqual(resp_data["flow_info"]["destination_port_id"], allocated_ports[1])
        self.assertEqual(resp_data["flow_info"]["target_app_name"], str(target_app))
        self.assertEqual(resp_data["flow_info"]["negotiated_qos"], {"reliability": "true"})

        # 2. Check notification sent to target
        notify_call = self.dif._send_management_pdu.call_args_list[1]
        notify_pdu, notify_addr = notify_call[0]

        self.assertEqual(notify_addr, target_underlying_address) # Sent to resolved target addr
        self.assertEqual(notify_pdu.pdu_type, PduType.MGMT_FLOW_SETUP_NOTIFY)
        notify_data = json.loads(notify_pdu.payload.decode('utf-8'))
        self.assertEqual(notify_data["source_app_name"], str(source_app))
        self.assertEqual(notify_data["source_port_id"], allocated_ports[0])
        self.assertEqual(notify_data["destination_port_id"], allocated_ports[1])
        self.assertEqual(notify_data["negotiated_qos"], {"reliability": "true"})

        # Check port allocation was called twice
        self.assertEqual(self.dif._allocate_port_id.call_count, 2)

    async def test_handle_flow_request_ipcp_not_found(self):
        """Test flow request when source or target IPCP is not found."""
        source_app = ApplicationName("App1")
        target_app = ApplicationName("AppNotFound") # Target doesn't exist
        tid = 457
        request_payload = json.dumps({
            "source_app_name": str(source_app),
            "target_app_name": str(target_app),
            "tid": tid
        }).encode('utf-8')
        request_pdu = PDU(pdu_type=PduType.MGMT_FLOW_ALLOC_REQ, dest_port_id=0, src_port_id=601, payload=request_payload)

        # Mock IPCP resolution (target returns None)
        self.dif.resolve_name = AsyncMock(side_effect=lambda name: self.mock_ipcp1 if name == source_app else None)
        self.dif._allocate_port_id = AsyncMock() # Should not be called
        self.dif._send_management_pdu = AsyncMock(return_value=True)

        await self.dif._handle_flow_request_pdu(request_pdu, self.source_addr)

        # Check only response is sent (no notification)
        self.dif._send_management_pdu.assert_awaited_once()
        resp_pdu, resp_addr = self.dif._send_management_pdu.call_args[0]

        self.assertEqual(resp_addr, self.source_addr)
        self.assertEqual(resp_pdu.pdu_type, PduType.MGMT_FLOW_ALLOC_RESP)
        resp_data = json.loads(resp_pdu.payload.decode('utf-8'))
        self.assertEqual(resp_data["status"], "failure")
        self.assertEqual(resp_data["tid"], tid)
        self.assertIn("Source or target application not found", resp_data["reason"])

        # Ensure ports were not allocated
        self.dif._allocate_port_id.assert_not_called()

    async def test_handle_flow_request_port_allocation_fails(self):
        """Test flow request when port allocation fails."""
        source_app = ApplicationName("App1")
        target_app = ApplicationName("App2")
        tid = 458
        request_payload = json.dumps({
            "source_app_name": str(source_app), "target_app_name": str(target_app), "tid": tid
        }).encode('utf-8')
        request_pdu = PDU(pdu_type=PduType.MGMT_FLOW_ALLOC_REQ, dest_port_id=0, src_port_id=602, payload=request_payload)

        self.dif.resolve_name = AsyncMock(side_effect=lambda name: self.mock_ipcp1 if name == source_app else (self.mock_ipcp2 if name == target_app else None))
        # Mock port allocation to fail (return None)
        self.dif._allocate_port_id = AsyncMock(return_value=None)
        self.dif._send_management_pdu = AsyncMock(return_value=True)
        self.dif._release_port_id = AsyncMock() # Mock release for cleanup check

        await self.dif._handle_flow_request_pdu(request_pdu, self.source_addr)

        self.dif._send_management_pdu.assert_awaited_once()
        resp_pdu, _ = self.dif._send_management_pdu.call_args[0]
        resp_data = json.loads(resp_pdu.payload.decode('utf-8'))
        self.assertEqual(resp_data["status"], "failure")
        self.assertEqual(resp_data["tid"], tid)
        self.assertIn("Failed to allocate port IDs", resp_data["reason"])

        # Check that release wasn't called (as allocation failed immediately)
        # Note: If allocation succeeded once then failed, release *would* be called.
        # Test that specific scenario separately if needed.
        self.dif._release_port_id.assert_not_called()


if __name__ == '__main__':
    unittest.main()