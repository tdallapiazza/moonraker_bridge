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
from enum import StrEnum
import logging

# https://github.com/cmroche/moonraker-api
# a good code quality project example https://github.com/frap129/klipmi

from typing import Callable, Coroutine, Dict, List, Literal

from moonraker_api import MoonrakerClient, MoonrakerListener
from moonraker_api.const import (
    WEBSOCKET_CONNECTION_TIMEOUT,
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


class PrinterState(StrEnum):
    NOT_READY = 'not ready'
    READY = 'ready'
    STOPPED = 'stopped'
    MOONRAKER_ERR = 'moonraker error'
    KLIPPER_ERR = 'klipper error'


class MoonrakerBridge(MoonrakerListener):

    def __init__(self):
        self.running = False
        self.client = MoonrakerClient(
            self,
            'localhost',
            7125,
        )
        self.objects = {
            'gcode_move': ['extrude_factor', 'speed_factor', 'homing_origin'],
            'motion_report': ['live_position', 'live_velocity'],
            'webhooks': ['state', 'state_message'],
            'fan': ['speed'],
            'heater_fan hotend_fan': ['speed'],
            'heater_bed': ['temperature', 'target', 'power'],
            'extruder': ['temperature', 'target', 'power'],
            'filament_switch_sensor Filament_Runout_Sensor': ['filament_detected'],
            'display_status': ['progress'],
            'print_stats': [
                'state',
                'print_duration',
                'filename',
                'total_duration',
                'info',
            ],
        }

    async def start(self) -> None:
        """Start the websocket connection."""
        self.running = True
        self.state = PrinterState.NOT_READY
        return await self.client.connect()

    async def stop(self) -> None:
        """Stop the websocket connection."""
        _LOGGER.info('Stopping')
        self.running = False
        await self.__updateState(PrinterState.STOPPED)
        await self.client.disconnect()

    async def state_changed(self, state: str) -> None:
        """Notifies of changing websocket state."""
        _LOGGER.debug('Stated changed to %s', state)
        tasks: List[Coroutine] = []
        printerStatus = PrinterState.NOT_READY
        if state == WEBSOCKET_STATE_CONNECTING:
            pass
        elif state == WEBSOCKET_STATE_CONNECTED:
            tasks.append(self.__subscribe())
            tasks.append(self.__updateKlippyStatus())
        elif state == WEBSOCKET_STATE_STOPPING:
            pass
        elif state == WEBSOCKET_STATE_STOPPED:
            _LOGGER.info('Websocket closed')
            printerStatus = PrinterState.STOPPED
        elif state == WEBSOCKET_CONNECTION_TIMEOUT:
            printerStatus = PrinterState.MOONRAKER_ERR
        tasks.append(self.__updateState(printerStatus))
        asyncio.gather(*tasks)

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

    async def __subscribe(self):
        await self.client.call_method('printer.objects.subscribe', objects=self.objects)


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
