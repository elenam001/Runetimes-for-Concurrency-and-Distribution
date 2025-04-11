import unittest
from rina.core.naming import ApplicationName, DIFName, PortId, IPCProcessAddress

class TestNaming(unittest.TestCase):

    def test_application_name(self):
        name = ApplicationName("MyApp/1")
        self.assertIsInstance(name, ApplicationName)
        self.assertIsInstance(name, str)
        self.assertEqual(name, "MyApp/1")

    def test_dif_name(self):
        name = DIFName("MyDIF")
        self.assertIsInstance(name, DIFName)
        self.assertIsInstance(name, str)
        self.assertEqual(name, "MyDIF")

    def test_port_id(self):
        port = PortId(12345)
        # Note: isinstance(port, PortId) won't work with NewType
        self.assertIsInstance(port, int)
        self.assertEqual(port, 12345)

if __name__ == '__main__':
    unittest.main()