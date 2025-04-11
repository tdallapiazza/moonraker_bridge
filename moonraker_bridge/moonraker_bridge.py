# Copyright 2025 Thomas Dalla Piazza.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


import asyncio
import logging

# https://github.com/cmroche/moonraker-api
# a good code quality project example https://github.com/straga/klipmi/blob/09ae17c51506a776897001c866d2d40cf97d23ec/src/klipmi/model/printer.py#L24

from moonraker_api import MoonrakerClient, MoonrakerListener
from moonraker_api.const import (
    WEBSOCKET_STATE_CONNECTED,
    WEBSOCKET_STATE_CONNECTING,
    WEBSOCKET_STATE_STOPPED,
    WEBSOCKET_STATE_STOPPING,
)
from moonraker_api.websockets.websocketclient import (
    ClientNotAuthenticatedError,
)

logging.basicConfig(
    level=logging.WARNING, format='%(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('moonraker_api').setLevel(logging.INFO)
logging.getLogger(__name__).setLevel(logging.INFO)
_LOGGER = logging.getLogger(__name__)


class MoonrakerBridge(MoonrakerListener):

    def __init__(self):
        self.running = False
        self.client = MoonrakerClient(
            self,
            'localhost',
            7125,
        )

    async def start(self) -> None:
        """Start the websocket connection."""
        self.running = True
        return await self.client.connect()

    async def stop(self) -> None:
        """Stop the websocket connection."""
        _LOGGER.info('Stopping')
        self.running = False
        await self.client.disconnect()

    async def state_changed(self, state: str) -> None:
        """Notifies of changing websocket state."""
        _LOGGER.debug('Stated changed to %s', state)
        if state == WEBSOCKET_STATE_CONNECTING:
            pass
        elif state == WEBSOCKET_STATE_CONNECTED:
            pass
        elif state == WEBSOCKET_STATE_STOPPING:
            pass
        elif state == WEBSOCKET_STATE_STOPPED:
            _LOGGER.info('Websocket closed. Try to reconnect...')
            if self.running:
                self.running = False
                _LOGGER.info('Disconnected.')
                _LOGGER.info('Re-connect...')
            await asyncio.sleep(2)
            await self.start()
            pass

    async def on_exception(self, exception: BaseException) -> None:
        """Notifies of exceptions from the websocket run loop."""
        _LOGGER.debug('Received exception from API websocket %s', str(exception))
        if isinstance(exception, ClientNotAuthenticatedError):
            self.entry.async_start_reauth(self.hass)
        else:
            raise exception

    async def on_notification(self, method: str, data: any) -> None:
        """Notifies of state updates."""
        if method != 'notify_proc_stat_update':
            _LOGGER.debug('Received notification %s -> %s', method, data)

        # Subscription notifications
        if method == 'notify_status_update':
            message = data[0]
            timestamp = data[1]
            _LOGGER.info('Received status update notnificatio %s -> %s', timestamp, message)
            # await self.process_status_message(message, timestamp)


async def main():
    bridge = MoonrakerBridge()
    await bridge.start()

    response = await bridge.client.call_method('printer.info')
    _LOGGER.info(response)
    # await bridge.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(), loop=loop)
    loop.run_forever()
