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
import nest_asyncio
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

from printer_interfaces.msg import PrinterState
from printer_interfaces.msg import HeaterBed
from printer_interfaces.msg import Extruder
from printer_interfaces.msg import MotionReport
from printer_interfaces.msg import Fans
from printer_interfaces.msg import FilamentSwitchSensor
from printer_interfaces.msg import PrintStats
from printer_interfaces.msg import DisplayStatus
from printer_interfaces.srv import SetBedTemperature
from printer_interfaces.srv import SetExtruderTemperature
from printer_interfaces.srv import ExecuteGCode
from printer_interfaces.srv import StartPrintJob
from printer_interfaces.srv import PausePrintJob
from printer_interfaces.srv import ResumePrintJob
from printer_interfaces.srv import CancelPrintJob
from printer_interfaces.srv import QueryEndStops


nest_asyncio.apply()


class Notifications(StrEnum):
    KLIPPY_READY = "notify_klippy_ready"
    KLIPPY_SHUTDOWN = "notify_klippy_shutdown"
    KLIPPY_DISCONNECTED = "notify_klippy_shutdown"
    STATUS_UPDATE = "notify_status_update"
    GCODE_RESPONSE = "notify_gcode_response"
    FILES_CHANGED = "notify_filelist_changed"

class MoonrakerBridge(MoonrakerListener,Node):

    def __init__(self):
        super().__init__('moonraker_bridge')
        # Publishers
        self.printer_state_publisher_ = self.create_publisher(PrinterState, f'{self.get_name()}/status/state', 10)
        self.heater_bed_publisher_ = self.create_publisher(HeaterBed, f'{self.get_name()}/status/heater_bed', 10)
        self.extruder_publisher_ = self.create_publisher(Extruder, f'{self.get_name()}/status/extruder', 10)
        self.motion_report_publisher_ = self.create_publisher(MotionReport, f'{self.get_name()}/status/motion_report', 10)
        self.fans_publisher_ = self.create_publisher(Fans, f'{self.get_name()}/status/fans', 10)
        self.print_stats_publisher_ = self.create_publisher(PrintStats, f'{self.get_name()}/status/print_stats', 10)
        self.filament_sensor_publisher_ = self.create_publisher(FilamentSwitchSensor, f'{self.get_name()}/status/filament_sensor', 10)
        self.display_status_publisher_ = self.create_publisher(DisplayStatus, f'{self.get_name()}/status/display_status', 10)

        # Services
        self.set_bed_temperature_srv = self.create_service(SetBedTemperature, f'{self.get_name()}/commands/set_bed_temperature', self.set_bed_temperature)
        self.set_extruder_temperature_srv = self.create_service(SetExtruderTemperature, f'{self.get_name()}/commands/set_extruder_temperature', self.set_extruder_temperature)
        self.execute_gcode_srv = self.create_service(ExecuteGCode, f'{self.get_name()}/commands/execute_gcode', self.execute_gcode)
        self.start_print_job_srv = self.create_service(StartPrintJob, f'{self.get_name()}/commands/start_print_job', self.start_print_job)
        self.pause_print_job_srv = self.create_service(PausePrintJob, f'{self.get_name()}/commands/pause_print_job', self.pause_print_job)
        self.resume_print_job_srv = self.create_service(ResumePrintJob, f'{self.get_name()}/commands/resume_print_job', self.resume_print_job)
        self.cancel_print_job_srv = self.create_service(CancelPrintJob, f'{self.get_name()}/commands/cancel_print_job', self.cancel_print_job)

        # Other members
        self.running = False
        self.status: dict = {}
        self.files: dict = {}
        self.client = MoonrakerClient(
            self,
            'localhost',
            7125,
        )

        # List of objects
        self.objects = {
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
                'total_duration'
            ],
        }

    def set_bed_temperature(self, request, response):
        params = {"script": f'SET_HEATER_TEMPERATURE HEATER=heater_bed TARGET={request.temperature}'}
        res = asyncio.run(self.client.call_method("printer.gcode.script", **params))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Setting bed temperature to : %s' % (request.temperature))
        return response
    
    def set_extruder_temperature(self, request, response):
        params = {"script": f'SET_HEATER_TEMPERATURE HEATER=extruder TARGET={request.temperature}'}
        res = asyncio.run(self.client.call_method("printer.gcode.script", **params))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Setting extruder temperature to : %s' % (request.temperature))
        return response
    
    def execute_gcode(self, request, response):
        params = {"script": request.commands}
        res = asyncio.run(self.client.call_method("printer.gcode.script", **params))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Executing gcode : %s' % (request.commands))
        return response
    
    def start_print_job(self, request, response):
        params = {"filename": request.filename}
        res = asyncio.run(self.client.call_method("printer.print.start", **params))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Starting print job : %s' % (request.filename))
        return response
    
    def pause_print_job(self, request, response):
        res = asyncio.run(self.client.call_method("printer.print.pause"))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Pause print job')
        return response
    
    def resume_print_job(self, request, response):
        res = asyncio.run(self.client.call_method("printer.print.resume"))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Resume print job')
        return response
    
    def cancel_print_job(self, request, response):
        res = asyncio.run(self.client.call_method("printer.print.cancel"))
        # res will include the error as a dictionary so cast in in a string
        response.result = str(res)
        self.get_logger().debug('Cancel print job')
        return response


    async def start(self) -> None:
        """Start the websocket connection. Recursively retry if can not connect."""
        conn=False
        try:
            conn = await self.client.connect()
        except:
            self.get_logger().warning('Could note connect to server. Retry in 5sec')
            await asyncio.sleep(5)
            await self.start()
        else:
            self.get_logger().info('Connection to server successfull')
            self.running = True
            self.state = PrinterState.NOT_READY
            self.prev_state = PrinterState.NOT_READY
        return conn

    async def stop(self) -> None:
        """Stop the websocket connection ant try to restart."""
        self.running = False
        await self.__updateState(PrinterState.STOPPED)
        await self.client.disconnect()
        # try to re-connect
        await self.start()

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
            tasks.append(self.stop())
            printerState = PrinterState.STOPPED
        elif state == WEBSOCKET_CONNECTION_TIMEOUT:
            printerState = PrinterState.MOONRAKER_ERR
        tasks.append(self.__updateState(printerState))
        await asyncio.gather(*tasks)

    async def on_exception(self, exception: BaseException) -> None:
        """Notifies of exceptions from the websocket run loop."""
        self.get_logger().warning('Received exception from API websocket %s' % (str(exception)))
        raise exception

    async def on_notification(self, method: str, data: any) -> None:
        """Notifies of state updates."""
        tasks: List[Coroutine] = []
        if method == Notifications.KLIPPY_READY:
            tasks.append(self.__subscribe())
            tasks.append(self.__updateKlippyStatus())
            tasks.append(self.__updateState(PrinterState.READY))
        elif method == Notifications.KLIPPY_SHUTDOWN:
            tasks.append(self.__updateKlippyStatus())
        elif method == Notifications.KLIPPY_DISCONNECTED:
            tasks.append(self.__updateKlippyStatus())
        elif method == Notifications.STATUS_UPDATE:
            updateNestedDict(self.status, data[0])
            updated_keys = data[0].keys()
            time = self.get_clock().now()
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
        self.get_logger().debug('Publishing PrinterState message : %s' % (msg.current_state))

    def publish_heater_bed(self, time):
        bed_dict = self.status['heater_bed']
        msg = HeaterBed()
        msg.stamp = time.to_msg()
        msg.temperature=bed_dict['temperature']
        msg.target = bed_dict['target']
        msg.power_pwm = bed_dict['power']
        self.heater_bed_publisher_.publish(msg)
        self.get_logger().debug('Publishing HeaterBed message : %s' % (msg))

    def publish_extruder(self, time):
        extruder_dict = self.status['extruder']
        msg = Extruder()
        msg.stamp = time.to_msg()
        msg.temperature=extruder_dict['temperature']
        msg.target = extruder_dict['target']
        msg.power_pwm = extruder_dict['power']
        self.extruder_publisher_.publish(msg)
        self.get_logger().debug('Publishing Extruder message : %s' % (msg))

    def publish_motion_report(self, time):
        motion_dict = self.status['motion_report']
        msg = MotionReport()
        msg.stamp = time.to_msg()
        msg.x=motion_dict['live_position'][0]
        msg.y=motion_dict['live_position'][1]
        msg.z=motion_dict['live_position'][2]
        msg.e=motion_dict['live_position'][3]
        msg.velocity = motion_dict['live_velocity']
        self.motion_report_publisher_.publish(msg)
        self.get_logger().debug('Publishing MotionReport message : %s' % (msg))

    def publish_fans(self, time):
        msg = Fans()
        msg.stamp = time.to_msg()
        msg.piece_cooling_fan_speed = self.status['fan']['speed']
        msg.heater_fan_speed = self.status['heater_fan heater_fan']['speed']
        msg.controller_fan_speed = self.status['controller_fan controller_fan']['speed']
        self.fans_publisher_.publish(msg)
        self.get_logger().debug('Publishing Fans message : %s' % (msg))

    def publish_filament_sensor(self, time):
        msg = FilamentSwitchSensor()
        msg.stamp = time.to_msg()
        msg.filament_detected = self.status['filament_switch_sensor runout_sensor']['filament_detected']
        self.filament_sensor_publisher_.publish(msg)
        self.get_logger().debug('Publishing FilamentSensor : %s' % (msg))

    def publish_display_status(self, time):
        msg = DisplayStatus()
        msg.stamp = time.to_msg()
        msg.progress = self.status['display_status']['progress']
        self.display_status_publisher_.publish(msg)
        self.get_logger().debug('Publishing DisplayStatus : %s' % (msg))

    def publish_print_stats(self, time):
        msg = PrintStats()
        print_stats = self.status['print_stats']
        msg.stamp = time.to_msg()
        msg.state = print_stats['state']
        msg.filename = print_stats['filename']
        msg.print_duration = print_stats['print_duration']
        msg.total_duration = print_stats['total_duration']
        self.print_stats_publisher_.publish(msg)
        self.get_logger().debug('Publishing PrintStats :%s' % (msg))

    def printer_callback(self, keys, time):
        for key in keys:
            if key == 'heater_bed':
                self.publish_heater_bed(time)
            elif key == 'extruder':
                self.publish_extruder(time)
            elif key == 'motion_report':
                self.publish_motion_report(time)
            elif key == 'fan' or key == 'heater_fan heater_fan' or key == 'controller_fan controller_fan':
                self.publish_fans(time)
            elif key == 'filament_switch_sensor runout_sensor':
                self.publish_filament_sensor(time)
            elif key == 'display_status':
                self.publish_display_status(time)
            elif key == 'print_stats':
                self.publish_print_stats(time)
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
        self.printer_callback(self.objects.keys(),self.get_clock().now())
        



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
    
