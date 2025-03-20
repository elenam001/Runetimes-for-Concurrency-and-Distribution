# ----------------------------
# Naming Registry for APNs
# Maps APNs to IPCP instances
# ----------------------------
import logging


class NamingRegistry:
    def __init__(self):
        self.registry = {}
    
    def register(self, apn, ipcp):
        self.registry[apn] = ipcp
        logging.info("Registered APN '%s' with IPCP instance.", apn)
    
    def resolve(self, apn):
        return self.registry.get(apn, None)

naming_registry = NamingRegistry()