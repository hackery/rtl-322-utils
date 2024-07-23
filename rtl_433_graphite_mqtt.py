#!/usr/bin/env python3
""" Receive rtl_433 JSON data from MQTT and send to carbon-cache
Run SDR receiver as: /usr/local/bin/rtl_433 -F mqtt
Outputs on topic prefix rtl_433/$hostname
"""

import logging
import pprint
import sys
import platform
import time
import datetime
import dateutil.parser as dp
import threading
import queue
import multiprocessing as mp
import paho.mqtt.client as mqtt
import json
import re
import socket
#from graphyte import Sender

log = mp.log_to_stderr()  # Multiprocessing capable logger
mqtt_client = mqtt.Client("RTL_433_CarbonCache")
exitevent = threading.Event()

# rtl_433 emits both individual key/value pairs for each reading, and a JSON event string -
# we use just the latter, with the existing parser.

MQTT_SERVER = "localhost"
MQTT_TOPIC = "rtl_433/%s/events" % platform.node()

# Config for sending to Carbon-Cache
CARBON_HOST = "localhost"
CARBON_PORT = 12003
#CARBON_PREFIX = "rtl_433"

SENDER_QUEUE_BACKOFF_MIN = 1
SENDER_QUEUE_BACKOFF_MAX = 60

#############################################################################

class CarbonSender:
    def __init__(self, host=CARBON_HOST, port=CARBON_PORT, queue_size=1000):
        self.host = host
        self.port = port
        
        self.s = None
        self.q = queue.Queue(maxsize=queue_size)
        self.q_backoff = SENDER_QUEUE_BACKOFF_MIN
        
        self.t = threading.Thread(target=lambda *args: self.drain_queue(*args)).start()
        
    def send(self, metric, value, timestamp):
        message = "xxx.{0} {1} {2}".format(metric, value, timestamp)
        self._send(message)

    def _send(self, message):
        if self.s is None:
            try:
                self.s = socket.create_connection( (self.host,self.port) )
                log.info("Connected to %s", self.host)
            except ConnectionRefusedError:
                log.error("Connection to Carbon-Cache refused")

        if self.s is not None: # connected, or reconnected
            try:
                self.s.sendall(str.encode(message+ "\n"))
            except BrokenPipeError:
                log.error("Lost connection to Carbon-Cache")
                self.s.close()
                self.s = None

        if self.s is None: # did not connect, or did and then failed to send
            log.info("[Q+] %s", message)
            self.q.put(message)
            return False

        return True

    def drain_queue(self):
        log.info("Drain queue start")
        while True:
            message = self.q.get() # blocks until available
            log.info("[Q-] %s", message)
            # Figure out how to re-send message safely:
            # send() will re-queue, and if we don't have a delay,
            # we immediately re-read, you want 100% cpu eh?
            # 0. Don't want a constant delay: too long to drain queue
            #    Some kind of exponential back-off?
            # 1. pre-emptively check that connection is live again?
            # 2. have a return value from send() indicating failure,
            #    and if so, then we back off?

            # Hacky first attempt might be good enough ...
            if self._send(message) is True: # successful send, reset backoff
                self.q_backoff = SENDER_QUEUE_BACKOFF_MIN
            else: # failed to send, so we delay retry
                time.sleep(self.q_backoff)
                if self.q_backoff < SENDER_QUEUE_BACKOFF_MAX:
                    self.q_backoff *= 2

#def on_connect(client, userdata, flags, rc):
#    log.info("Connected (1)")

class MqttReceiver:
    def __init__(self):
        #mqtt_client.on_connect = on_connect
        mqtt_client.on_connect = lambda *args: self.on_connect(*args)
        mqtt_client.on_disconnect = lambda *args: self.on_disconnect(*args)
        mqtt_client.on_message = lambda *args: self.on_message(*args)

    def run(self):
        # self.sender = Sender(CARBON_HOST, prefix=CARBON_PREFIX, log_sends=True)
        self.sender = CarbonSender()

        mqtt_client.connect(MQTT_SERVER)
        mqtt_client.loop_start()
        exitevent.wait()

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error("Could not connect: %d", rc)
            exitevent.set()

        log.info("Connected to MQTT: %s", mqtt.connack_string(rc))
        log.info("Subscribing to topic %s", MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC) # subscribe takes a prefix, but there's no sub-level for "event"


    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.error("Disconnected: %d", rc)


    def on_message(self, client, userdata, msg):
        if msg.topic == MQTT_TOPIC:
            log.debug("topic %s : %s", msg.topic, msg.payload.decode())
            try:
                message = json.loads(msg.payload.decode())
            except json.decoder.JSONDecodeError:
                log.warning("JSON decode error: %s", msg.payload.decode())
                return
            except Exception as e:
                log.warning("Some error: %s", e)
                return

            #log.info(pprint.pformat(message))
            #log.info("Message time: %s", message['time'])
            self.parse(message)

    def send(self, metric, value, time):
          # now = int(time.time())
          print("{} {} {}".format(metric, value, time))
          self.sender.send(metric, value, time)

    def parse(self, message):
        try:
            if all(attr in message for attr in ['time', 'model']):
               time = dp.parse(message['time']).strftime("%s")
               if re.search("CurrentCost.TX", message['model']):
                   self.send("currentcost.power0",      message['power0_W'], time)
                   self.send("currentcost.%d.power0" %  message['id'], message['power0_W'], time)

               if re.search("THGR810", message['model']):
                   self.send("environment.thgr810.%s.temperature_C"  % message['id'], message['temperature_C'], time)
                   self.send("environment.thgr810.%s.humidity"       % message['id'], message['humidity'],      time)
                   self.send("environment.thgr810.%s.battery_ok"     % message['id'], message['battery_ok'] == "OK", time)

               if re.search("THGR122N", message['model']):
                   self.send("environment.thgr122n.%s.temperature_C" % message['id'], message['temperature_C'], time)
                   self.send("environment.thgr122n.%s.humidity"      % message['id'], message['humidity'],      time)
                   self.send("environment.thgr122n.%s.battery_ok"    % message['id'], message['battery_ok'] == "OK", time)

               if re.search("PCR800", message['model']):
                   self.send("environment.pcr800.%s.rain_rate_in_h"  % message['id'], message['rain_rate_in_h'], time)
                   self.send("environment.pcr800.%s.rain_in"         % message['id'], message['rain_in'],        time)
                   self.send("environment.pcr800.%s.battery_ok"      % message['id'], message['battery_ok'] == "OK", time)

               if re.search("WGR800", message['model']):
                   self.send("environment.wgr800.%s.average"         % message['id'], message['wind_avg_m_s'], time)
                   self.send("environment.wgr800.%s.gust"            % message['id'], message['wind_max_m_s'], time)
                   self.send("environment.wgr800.%s.direction"       % message['id'], message['wind_dir_deg'], time)
                   self.send("environment.wgr800.%s.battery_ok"      % message['id'], message['battery_ok'] == "OK", time)

               if re.search("Fineoffset-WH51", message['model']):
                   self.send("environment.wh51.%s.battery_ok"        % message['id'], message['battery_ok'], time)
                   self.send("environment.wh51.%s.battery_mV"        % message['id'], message['battery_mV'], time)
                   self.send("environment.wh51.%s.moisture"          % message['id'], message['moisture'],   time)

        except KeyError as e:
          log.error("Key error: %s", e)
        except Exception as e:
          log.error("Error in parse/send %s: %s", type(e).__name__, e)


def main():
    """MQTT Carbon-Cache Relay"""
    log.setLevel(logging.INFO)
    log.info("MQTT RTL_433 Carbon-Cache Relay connecting to %s ...", MQTT_SERVER)

    mqtt_rx = MqttReceiver()
    mqtt_rx.run()

if __name__ == "__main__":
    main()
