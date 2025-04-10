import logging
import asyncio
from moonraker_api import MoonrakerListener, MoonrakerClient
from moonraker_api.const import (
    WEBSOCKET_STATE_CONNECTING,
    WEBSOCKET_STATE_CONNECTED,
    WEBSOCKET_STATE_STOPPING,
    WEBSOCKET_STATE_STOPPED
)
from moonraker_api.websockets.websocketclient import (
    ClientNotAuthenticatedError,
)

logging.basicConfig(
    level=logging.WARNING, format="%(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("moonraker_api").setLevel(logging.INFO)
logging.getLogger(__name__).setLevel(logging.INFO)
_LOGGER = logging.getLogger(__name__)

class MoonrakerBridge(MoonrakerListener):
    def __init__(self):
        self.running = False
        self.client = MoonrakerClient(
            self,
            "localhost",
            7125,
        )

    async def start(self) -> None:
        """Start the websocket connection."""
        self.running = True
        return await self.client.connect()

    async def stop(self) -> None:
        """Stop the websocket connection."""
        self.running = False
        await self.client.disconnect()
    

    async def state_changed(self, state: str) -> None:
        """Notifies of changing websocket state."""
        _LOGGER.debug("Stated changed to %s", state)
        if state == WEBSOCKET_STATE_CONNECTING:
            pass
        elif state == WEBSOCKET_STATE_CONNECTED:
            pass
        elif state == WEBSOCKET_STATE_STOPPING:
            pass
        elif state == WEBSOCKET_STATE_STOPPED:
            _LOGGER.info("Websocket closed. Stopping services")
            pass

    async def on_exception(self, exception: BaseException) -> None:
        """Notifies of exceptions from the websocket run loop."""
        _LOGGER.debug("Received exception from API websocket %s", str(exception))
        if isinstance(exception, ClientNotAuthenticatedError):
            self.entry.async_start_reauth(self.hass)
        else:
            raise exception

    async def on_notification(self, method: str, data: any) -> None:
        """Notifies of state updates."""

        if method!= "notify_proc_stat_update":
            _LOGGER.debug("Received notification %s -> %s", method, data)

        # Subscription notifications
        if method == "notify_status_update":
            message = data[0]
            timestamp = data[1]
            _LOGGER.info("Received status update notnificatio %s -> %s", timestamp, message)
            #await self.process_status_message(message, timestamp)

async def main():
    bridge = MoonrakerBridge()
    await bridge.start()

    response = await bridge.client.call_method("printer.info")
    _LOGGER.info(response)
    await bridge.stop()


if __name__ == '__main__':
    asyncio.run(main())

