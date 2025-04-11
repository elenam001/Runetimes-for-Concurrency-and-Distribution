import asyncio
import logging

from rina.management.layer_manager import LayerManager
from rina.interfaces.network_interface import UdpInterface
from rina.core.naming import ApplicationName, DIFName, PortId

# --- Logging Configuration ---
# Set level to DEBUG to see detailed PDU flow, timers, etc.
# Set to INFO for higher-level steps.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d - %(levelname)-7s - %(name)-15s - %(message)s',
    datefmt='%H:%M:%S'
)
# Optionally reduce verbosity of libraries if needed
# logging.getLogger("asyncio").setLevel(logging.WARNING)
log = logging.getLogger("main")

# --- Application Logic Definition ---

# Use a dictionary to store received messages for verification later if needed
received_messages = {}

async def simple_app_handler(app_name: ApplicationName, port_id: PortId, data: bytes):
    """
    Generic handler for simple applications. Logs received data.
    This function will be called by the IPCP's 'deliver_to_app'.
    """
    global received_messages
    try:
        message = data.decode('utf-8')
        log.info(f"*** App Handler [{app_name}] (Flow {port_id}): Received message: '{message}' ***")
        # Store message for potential verification
        if app_name not in received_messages:
            received_messages[app_name] = []
        received_messages[app_name].append(message)
    except UnicodeDecodeError:
        log.info(f"*** App Handler [{app_name}] (Flow {port_id}): Received {len(data)} bytes of non-UTF-8 data ***")
    except Exception as e:
        log.error(f"*** App Handler [{app_name}] Error: {e} ***", exc_info=True)


async def run_simulation():
    """Sets up and runs the RINA stack simulation."""
    log.info("--- Starting RINA Simulation ---")
    layer_manager = LayerManager()

    # --- Configuration ---
    # Define parameters for easy modification

    # UDP Interface (Base Layer - DIF 0)
    # Port must match PLACEHOLDER_ADDRESS_MAP for dif0 components in layer_manager.py
    UDP_IF_ID = "udp0"
    UDP_IP = "127.0.0.1"
    UDP_PORT = 8000

    # DIF Names
    DIF0_NAME = DIFName("dif0")
    NDF_NAME = DIFName("normal_dif") # Normal DIF on top of DIF0

    # IPCP Names (match conventions used in LayerManager)
    NODE0_NAME = ApplicationName(f"{DIF0_NAME}-node") # DIF0's node IPCP
    APP1_NAME = ApplicationName("App1") # Application in NDF
    APP2_NAME = ApplicationName("App2") # Application in NDF

    # --- Setup Phase ---
    try:
        # 1. Create and Register Network Interface
        log.info(f"Setting up UDP Interface '{UDP_IF_ID}' on {UDP_IP}:{UDP_PORT}")
        udp_interface = UdpInterface(UDP_IF_ID, UDP_IP, UDP_PORT)
        await layer_manager.add_network_interface(UDP_IF_ID, udp_interface)

        # 2. Create DIF 0 ("the UDP network DIF")
        log.info(f"Creating Base DIF: {DIF0_NAME}")
        dif0 = await layer_manager.create_dif(DIF0_NAME)
        if not dif0: raise RuntimeError("Failed to create DIF 0")

        # 3. Create Node IPCP for DIF 0
        # This IPCP handles traffic for the UDP interface
        log.info(f"Creating Node IPCP: {NODE0_NAME} (for {DIF0_NAME})")
        node0 = await layer_manager.create_ipcp(NODE0_NAME, DIF0_NAME)
        if not node0: raise RuntimeError("Failed to create Node 0 IPCP")

        # 4. Stack DIF 0 over the UDP Interface
        # This connects the interface to node0's receive method
        log.info(f"Stacking {DIF0_NAME} over {UDP_IF_ID}")
        await layer_manager.stack_dif_over(DIF0_NAME, UDP_IF_ID)

        # 5. Create Normal DIF (NDF)
        log.info(f"Creating Normal DIF: {NDF_NAME}")
        ndf = await layer_manager.create_dif(NDF_NAME)
        if not ndf: raise RuntimeError("Failed to create NDF")

        # 6. Create Application IPCPs targeting NDF
        log.info(f"Creating Application IPCP: {APP1_NAME} (for {NDF_NAME})")
        app1 = await layer_manager.create_ipcp(
            APP1_NAME,
            NDF_NAME,
            # Use lambda to pass app_name context to the generic handler
            app_handle=lambda port, data: simple_app_handler(APP1_NAME, port, data)
        )
        if not app1: raise RuntimeError("Failed to create App1 IPCP")

        log.info(f"Creating Application IPCP: {APP2_NAME} (for {NDF_NAME})")
        app2 = await layer_manager.create_ipcp(
            APP2_NAME,
            NDF_NAME,
            app_handle=lambda port, data: simple_app_handler(APP2_NAME, port, data)
        )
        if not app2: raise RuntimeError("Failed to create App2 IPCP")

        # 7. Stack NDF over DIF 0
        # This assigns node0 as the underlying transport for app1, app2, and ndf management
        log.info(f"Stacking {NDF_NAME} over {DIF0_NAME}")
        await layer_manager.stack_dif_over(NDF_NAME, DIF0_NAME)

        # --- Start All Components ---
        log.info("Starting all components (Interfaces, DIFs, IPCPs)...")
        await layer_manager.start_all()
        # Add a small delay to allow components (especially network interface) to settle
        await asyncio.sleep(0.5)
        log.info("All components started.")

        # --- Run Test Scenario ---
        log.info("--- Running Test Scenario ---")

        # 8. Enroll IPCPs (sequentially for clearer logs, can be parallel)
        log.info(f"Enrolling {NODE0_NAME} into {DIF0_NAME}...")
        node0_enrolled = await layer_manager.enroll_ipcp(NODE0_NAME)
        log.info(f"-> {NODE0_NAME} Enrollment: {'SUCCESS' if node0_enrolled else 'FAILED'}")
        if not node0_enrolled: raise RuntimeError(f"{NODE0_NAME} enrollment failed, cannot proceed.")
        await asyncio.sleep(0.5) # Allow time for processing

        log.info(f"Enrolling {APP1_NAME} into {NDF_NAME}...")
        app1_enrolled = await layer_manager.enroll_ipcp(APP1_NAME)
        log.info(f"-> {APP1_NAME} Enrollment: {'SUCCESS' if app1_enrolled else 'FAILED'}")
        await asyncio.sleep(0.5)

        log.info(f"Enrolling {APP2_NAME} into {NDF_NAME}...")
        app2_enrolled = await layer_manager.enroll_ipcp(APP2_NAME)
        log.info(f"-> {APP2_NAME} Enrollment: {'SUCCESS' if app2_enrolled else 'FAILED'}")
        await asyncio.sleep(0.5)

        if not (app1_enrolled and app2_enrolled):
             raise RuntimeError("App enrollment failed, cannot proceed.")

        # 9. Allocate Flow from App1 to App2
        log.info(f"{APP1_NAME} requesting flow to {APP2_NAME}...")
        # Use the 'app1' IPCP object returned by create_ipcp
        flow_port_id = await app1.allocate_flow(APP2_NAME) # Target name

        if flow_port_id:
            log.info(f"-> Flow allocated for {APP1_NAME}! Local PortId: {flow_port_id}")
            await asyncio.sleep(0.5) # Allow time for potential target notification

            # 10. Send Data from App1 to App2
            message1 = "Hello RINA from App1!"
            log.info(f"{APP1_NAME} sending on flow {flow_port_id}: '{message1}'")
            sent1 = await app1.send_data(flow_port_id, message1.encode('utf-8'))
            log.info(f"-> {APP1_NAME} send attempt 1: {'Initiated' if sent1 else 'Failed'}")
            # Note: `sent` being True only means it was handed off for sending,
            # delivery depends on the (simple) reliable protocol in Flow.

            # Wait for potential delivery and ACK processing
            log.info("Waiting briefly for data transfer...")
            await asyncio.sleep(3.0) # Adjust based on expected RTT + processing

            # Send another message
            message2 = "This is the second message."
            log.info(f"{APP1_NAME} sending on flow {flow_port_id}: '{message2}'")
            sent2 = await app1.send_data(flow_port_id, message2.encode('utf-8'))
            log.info(f"-> {APP1_NAME} send attempt 2: {'Initiated' if sent2 else 'Failed'}")

            log.info("Waiting for final data transfer...")
            await asyncio.sleep(5.0) # Allow time for second message and potential ACKs

            # 11. Deallocate Flow (Optional but good practice)
            log.info(f"{APP1_NAME} deallocating flow {flow_port_id}")
            await app1.deallocate_flow(flow_port_id)
            await asyncio.sleep(0.5)

        else:
            log.error(f"!!! Flow allocation from {APP1_NAME} to {APP2_NAME} failed. Skipping data transfer. !!!")

        log.info("--- Scenario Complete ---")
        await asyncio.sleep(1) # Final pause before shutdown

    except asyncio.CancelledError:
         log.info("Simulation task cancelled.")
    except Exception as e:
        # Log any unexpected errors during setup or scenario execution
        log.critical(f"!!! Simulation failed with error: {e} !!!", exc_info=True)
    finally:
        # --- Shutdown Phase ---
        log.info("--- Shutting down RINA Simulation ---")
        # Ensure LayerManager stop is always called
        await layer_manager.stop_all()
        log.info("--- Simulation Finished ---")


# --- Main Execution ---
if __name__ == "__main__":
    log.info("Starting asyncio event loop for RINA simulation...")
    try:
        # Run the main simulation coroutine
        asyncio.run(run_simulation())
    except KeyboardInterrupt:
        log.info("\nSimulation interrupted by user (Ctrl+C).")
    except Exception as e:
         log.exception(f"An unhandled error occurred outside run_simulation: {e}")
    finally:
        log.info("Event loop finished.")