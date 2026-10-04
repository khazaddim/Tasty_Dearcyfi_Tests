"""Credential-free compatibility smoke test for the Tastytrade demo import surface."""

import importlib
import sys
import unittest
from unittest.mock import patch


SDK_MODULES = (
    "tastytrade",
    "tastytrade.session",
    "tastytrade.account",
    "tastytrade.market_data",
    "tastytrade.streamer",
    "tastytrade.dxfeed",
    "tastytrade.instruments",
    "tastytrade.order",
)


class TastytradeCompatibilityImportsTest(unittest.TestCase):
    def test_demo_sdk_modules_import_without_network_access(self) -> None:
        network_events: list[tuple[object, ...]] = []

        def record_network_event(event: str, arguments: tuple[object, ...]) -> None:
            if event == "socket.connect":
                network_events.append(arguments)

        # sys.addaudithook registers a callback for Python runtime events. For
        # "socket.connect", Python calls it with the event name and event data
        # (including the socket and destination address) when code tries to
        # connect a socket.
        #
        # We use it here as a second check: patching socket.create_connection
        # catches the usual helper, while this hook can notice code that calls
        # socket.connect directly. The callback records the event so the
        # assertion below fails if an import tries to connect.
        #
        # Audit hooks stay installed for the rest of this Python process, so
        # keep this callback small and limited to recording the event. It only
        # observes connection attempts; it does not block them.
        sys.addaudithook(record_network_event)
        print("Importing the Tastytrade demo SDK surface without creating a session:")
        with patch("socket.create_connection") as create_connection:
            for module_name in SDK_MODULES:
                with self.subTest(module=module_name):
                    print(f"  importing {module_name}")
                    importlib.import_module(module_name)

        create_connection.assert_not_called()
        self.assertEqual(network_events, [])
        print("Verified: all imports completed with no socket connections.")


if __name__ == "__main__":
    unittest.main()
