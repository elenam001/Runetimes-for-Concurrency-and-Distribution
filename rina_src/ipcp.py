# ----------------------------
# IPC Process (IPCP)
# Creates IPCP instances for each APN
# ----------------------------
import json
import time
import naming_registry
import protocol


class IPCP:
    def __init__(self, apn, dif):
        self.apn = apn
        self.dif = dif
        self.flows = {}
        naming_registry.register(apn, self)
    
    def handle_message(self, message, client_sock, client_addr):
        try:
            timestamp, flow_id, payload = protocol.unpack_message(message)
            if payload == b"FLOW_REQ":
                new_flow_id = f"{self.apn}-flow-{time.time()}"
                response = protocol.pack_message(
                    flow_id=new_flow_id,
                    payload=b"FLOW_OK"
                )
                client_sock.sendall(response)
                self.flows[new_flow_id] = (client_addr, time.time())
        except:
            message = json.loads(message.decode())
            msg_type = message.get("type")
            if msg_type == "flow_allocation_request":
                flow_id = f"{self.apn}-flow-{time.time()}"
                response = json.dumps({"flow_id": flow_id}).encode()
                client_sock.sendall(response)
            elif msg_type == "data_transfer":
                flow_id = message.get("flow_id")  # Ensure this line exists!
                response = json.dumps({"type": "ack", "flow_id": flow_id}).encode()
                client_sock.sendall(response)


    def handle_binary_message(self, timestamp, flow_id, payload, sock):
        if payload.startswith(b"REQ:"):
            # Flow allocation logic
            new_flow_id = f"{self.apn}-flow-{time.time()}"
            response = protocol.pack_message(flow_id=new_flow_id, payload=b"ACK")
            sock.sendall(response)
            self.flows[new_flow_id] = (sock.getpeername(), time.time())
        elif payload == b"TEARDOWN":
            if flow_id in self.flows:
                del self.flows[flow_id]
                sock.sendall(protocol.pack_message(flow_id=flow_id, payload=b"TEARDOWN_ACK"))
        else:
            response = protocol.pack_message(flow_id=flow_id, payload=b"ACK")
            sock.sendall(response)
