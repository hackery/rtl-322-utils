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
exitevent = threading.Event()

# rtl_433 emits both individual key/value pairs for each reading, and a JSON event string -
# we use just the latter, with the existing parser.

MQTT_SERVER = "localhost"
# MQTT_TOPIC = "rtl_433/%s/events" % platform.node()
MQTT_TOPIC = "rtl_433/+/events"

# Config for sending to Carbon-Cache
CARBON_HOST = "localhost"
CARBON_PORT = 2003
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
        message = "{0} {1} {2}".format(metric, value, timestamp)
        self._send(message)

    def _send(self, message):
        if self.s is None:
            try:
                self.s = socket.create_connection( (self.host,self.port) )
                log.info("Connected to %s", self.host)
            except ConnectionRefusedError:
                pass # do not log it, could be high volume. Logging first failure would be better.

        if self.s is not None: # connected, or reconnected
            try:
                self.s.sendall(str.encode(message+ "\n"))
            except BrokenPipeError:
                log.error("Lost connection to Carbon-Cache")
                self.s.close()
                self.s = None

        if self.s is None: # did not connect, or did and then failed to send
            log.debug("[Q+] %s", message)
            self.q.put(message)
            return False

        return True

    def drain_queue(self):
        log.debug("Drain queue start")
        while True:
            message = self.q.get() # blocks until available
            log.debug("[Q-] %s", message)
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
        self.mqtt_client = mqtt.Client("RTL_433_CarbonCache")
        self.mqtt_client.on_connect = lambda *args: self.on_connect(*args)
        self.mqtt_client.on_disconnect = lambda *args: self.on_disconnect(*args)

        self.mqtt_client.message_callback_add(MQTT_TOPIC, lambda *args: self.on_message(*args))

    def run(self):
        # self.sender = Sender(CARBON_HOST, prefix=CARBON_PREFIX, log_sends=True)
        self.sender = CarbonSender()

        self.mqtt_client.connect(MQTT_SERVER)
        self.mqtt_client.loop_start()
        exitevent.wait()

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error("Could not connect: %d", rc)
            exitevent.set() # do we exit here? or will the paho client retry?

        log.info("Connected to MQTT: %s", mqtt.connack_string(rc))
        log.info("Subscribing to topic %s", MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC) # subscribe takes a prefix, but there's no sub-level for "event"


    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.error("Disconnected: %d", rc)


    def on_message(self, client, userdata, msg):
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
        log.debug("{} {} {}".format(metric, value, time))
        self.sender.send(metric, value, time)

    def parse(self, message):
        try:
            if all(attr in message for attr in ['time', 'model', 'id']):
               time = dp.parse(message['time']).strftime("%s")

               model = message['model'].strip().lower()
               model = re.sub(r"^(Oregon|Fineoffset|LaCrosse)-", "", model, flags=re.IGNORECASE)
               model = re.sub(r"^CurrentCost.TX", "currentcost",     model, flags=re.IGNORECASE)

# Typical message looks like:
# {"time":"2023-11-06 11:40:15","brand":"OS","model":"Oregon-BTHGN129","id":114,"channel":1,"battery_ok":1,"temperature_C":18.1,"humidity":69,"pressure_hPa":995.0}

               for attr in set(message) - { 'time', 'brand', 'model', 'id', 'channel' }:
                   if type(message[attr]) in [int,float]:
                       self.send("environment.%s.%s.%s" % (model, message['id'], attr), message[attr], time)

               # some special cases for backward compatibility:

               if re.search("CurrentCost.TX", message['model']):
                   self.send("currentcost.power0",      message['power0_W'], time)  ###
                   self.send("currentcost.%d.power0" %  message['id'], message['power0_W'], time)  ###

               if re.search("WGR800", message['model']):
                   self.send("environment.wgr800.%s.average"         % message['id'], message['wind_avg_m_s'], time)  ###
                   self.send("environment.wgr800.%s.gust"            % message['id'], message['wind_max_m_s'], time)  ###
                   self.send("environment.wgr800.%s.direction"       % message['id'], message['wind_dir_deg'], time)  ###
                   self.send("environment.wgr800.%s.battery_ok"      % message['id'], message['battery_ok'],   time)

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
