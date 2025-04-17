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
from .utils import updateNestedDict

import rclpy
from rclpy.node import Node

from std_msgs.msg import String
from printer_interfaces.msg import PrinterState
from printer_interfaces.msg import HeaterBed

class Notifications(StrEnum):
    KLIPPY_READY = "notify_klippy_ready"
    KLIPPY_SHUTDOWN = "notify_klippy_shutdown"
    KLIPPY_DISCONNECTED = "notify_klippy_shutdown"
    STATUS_UPDATE = "notify_status_update"
    GCODE_RESPONSE = "notify_gcode_response"
    FILES_CHANGED = "notify_filelist_changed"

class MoonrakerBridge(MoonrakerListener,Node):

    def __init__(self):
        super().__init__('moonraker_publisher')
        self.printer_state_publisher_ = self.create_publisher(PrinterState, 'printer/state', 10)
        self.heater_bed_publisher_ = self.create_publisher(HeaterBed, 'printer/heater_bed', 10)
        self.running = False
        self.status: dict = {}
        self.files: dict = {}
        self.client = MoonrakerClient(
            self,
            'localhost',
            7125,
        )
        self.objects = {
            'gcode_move': ['extrude_factor', 'speed_factor', 'homing_origin'],
            'motion_report': ['live_position', 'live_velocity'],
            'fan': ['speed'],
            'heater_fan heater_fan': ['speed'],
            'controller_fan controller_fan': ['speed'],
            'heater_bed': ['temperature', 'target', 'power'],
            'extruder': ['temperature', 'target', 'power'],
            'filament_switch_sensor runout_sensor': ['filament_detected'],
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
        self.prev_state = PrinterState.NOT_READY
        return await self.client.connect()

    async def stop(self) -> None:
        """Stop the websocket connection."""
        self.running = False
        await self.__updateState(PrinterState.STOPPED)
        await self.client.disconnect()

    async def state_changed(self, state: str) -> None:
        """Notifies of changing websocket state."""
        self.get_logger().debug('Stated changed to %s' % (state))
        tasks: List[Coroutine] = []
        printerState = PrinterState.NOT_READY
        if state == WEBSOCKET_STATE_CONNECTING:
            pass
        elif state == WEBSOCKET_STATE_CONNECTED:
            tasks.append(self.__subscribe())
            tasks.append(self.__updateKlippyStatus())
        elif state == WEBSOCKET_STATE_STOPPING:
            pass
        elif state == WEBSOCKET_STATE_STOPPED:
            self.get_logger().warning('Websocket closed')
            printerState = PrinterState.STOPPED
        elif state == WEBSOCKET_CONNECTION_TIMEOUT:
            printerState = PrinterState.MOONRAKER_ERR
        tasks.append(self.__updateState(printerState))
        await asyncio.gather(*tasks)

    async def on_exception(self, exception: BaseException) -> None:
        """Notifies of exceptions from the websocket run loop."""
        self.get_logger().warning('Received exception from API websocket %s', str(exception))
        raise exception

    async def on_notification(self, method: str, data: any) -> None:
        """Notifies of state updates."""
        tasks: List[Coroutine] = []
        if method == Notifications.KLIPPY_READY:
            tasks.append(self.__subscribe())
            tasks.append(self.__updateKlippyStatus())
            tasks.append(self.__updateState(PrinterState.READY))
        elif method == Notifications.KLIPPY_SHUTDOWN:
            tasks.append(self.__updateState(PrinterState.KLIPPER_ERR))
        elif method == Notifications.KLIPPY_DISCONNECTED:
            tasks.append(self.__updateState(PrinterState.KLIPPER_ERR))
        elif method == Notifications.STATUS_UPDATE:
            updateNestedDict(self.status, data[0])
            updated_keys = data[0].keys()
            time = data[1]
            tasks.append(self.printer_callback(updated_keys, time))
        elif method == Notifications.FILES_CHANGED:
            self.files = data[0]
            tasks.append(self.files_callback(self.files))
        await asyncio.gather(*tasks)

    def publish_state(self, state, prev_state, time):
        msg = PrinterState()
        msg.stamp = time.to_msg()
        msg.previous_state=prev_state
        msg.current_state = state
        self.printer_state_publisher_.publish(msg)
        self.get_logger().info('Publishing PrinterState message : %s' % (msg.current_state))

    def publish_heater_bed(self, time):
        bed_dict = self.status['heater_bed']
        msg = HeaterBed()
        msg.stamp = time.to_msg()
        msg.temperature=bed_dict['temperature']
        msg.target = bed_dict['target']
        msg.power_pwm = bed_dict['power']
        self.heater_bed_publisher_.publish(msg)
        self.get_logger().info('Publishing HeaterBed message : %s' % (msg))

    def printer_callback(self, keys, time):
        for key in keys:
            if key == 'heater_bed':
                self.publish_heater_bed(time)
            else:
                self.get_logger().warning('No publisher implemented for key: %s' % (key))

    def files_callback(self, files):
        self.get_logger().warning('Files callback triggered but not implemented')

    async def __subscribe(self):
        await self.client.call_method('printer.objects.subscribe', objects=self.objects)
        self.get_logger().info('Moonraker subscriptions setup.')

    async def __updateState(self, state: PrinterState):
        if state != self.state:
            self.prev_state = self.state
            self.state = state
            self.publish_state(state, self.prev_state, self.get_clock().now())
    
    async def __updateKlippyStatus(self):
        state = await self.client.get_klipper_status()
        if state == "ready":
            self.get_logger().info('The printer is ready')
            await self.__updatePrinterStatus()
            await self.__updateState(PrinterState.READY)
        elif state == "shutdown" or state == "disconnected":
            self.get_logger().warning('The printer is on error!')
            await self.__updateState(PrinterState.KLIPPER_ERR)

    async def __updatePrinterStatus(self):
        self.get_logger().info('Querying printer status')
        self.status = (
            await self.client.call_method("printer.objects.query", objects=self.objects)
        )["status"]
        self.get_logger().debug('Printer status obtained:\n %s' % (self.status))
        self.publish_heater_bed(self.get_clock().now())
        



async def run(args=None):
    rclpy.init(args=args)
    bridge = MoonrakerBridge()
    await bridge.start()
    while rclpy.ok():
        rclpy.spin_once(bridge, timeout_sec=0)
        await asyncio.sleep(1e-4)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        asyncio.ensure_future(run(), loop=loop)
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
    
