#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMA Energy Meter Integration into Home Assistant via MQTT

The script needs a configuration file config.ini to retrieve details to 
connect to the MQTT broker. Reporting frequency as well as logging to file
can also be specified via the config file. 

The script can be called with the following options:
    -v (--verbose): to increase the output verbosity
    -d (--debug): to show debug output
    -c (--config_dir): to set directory where config.ini is located. 
    Defaults to directory of script file

The script uses the module 'smaem_decoder' to decode the udp telegram
message received from the SMA Energy Meter. Details on the message content
can be found in the respective script. For the time being only 2 counters,
'total_energy_consumed' and 'total_energy_supplied', are configured for
reporting to homeassistant. Script can be extended to report more values. 
Use debug option to console or file to see full list of available values 
and counters.
"""

#  load necessary libraries
import _thread
import socket
import struct
import sdnotify
from unidecode import unidecode
import os
import sys
import ssl
import json
import argparse
import threading
import logging
from logging.handlers import TimedRotatingFileHandler
import colorlog
from configparser import ConfigParser
from smaem_decoder import decode_SMAEM
from tzlocal import get_localzone
from time import sleep, localtime, strftime
from datetime import datetime
from collections import OrderedDict
import paho.mqtt.client as mqtt

script_version = "1.5.3"
script_name = "smaem-reporter.py"
script_info = f"{script_name} v{script_version}"
project_name = "SMA Energy Meter Integration into Home Assistant via MQTT"
project_url = "https://github.com/ufankhau/smaem"


#  define root logger 'log'
log = logging.getLogger()

if False:
    # will be caught by python 2.7 to be illegal syntax
    log.error("Sorry, this script requires a python3 runtime environment")
    os._exit(1)


#  construct the argument parser and parse the arguments
ap = argparse.ArgumentParser(description=project_name)
ap.add_argument(
    "-v", "--verbose", help="increase output verbosity", action="store_true"
)
ap.add_argument("-d", "--debug", help="show debug output", action="store_true")
ap.add_argument(
    "-c",
    "--config_dir",
    help="set directory where config.ini is located",
    default=sys.path[0],
)
args = vars(ap.parse_args())
opt_verbose = args["verbose"]
opt_debug = args["debug"]
config_dir = args["config_dir"]


#  CONSTANTS
ALIVE_TIMEOUT_IN_SECONDS = 60
TIMER_INTERRUPT = -1

#  initialize variables
local_tz = get_localzone()
smaserials = ""
ch_config = {}
fh_config = {}

#  settings for logging to console
if opt_verbose or opt_debug:
    ch_config["to_console"] = True
    if opt_debug:
        ch_config["level"] = "DEBUG"
    else:
        ch_config["level"] = "INFO"
else:
    ch_config["to_console"] = False


#  ********************************************************
#                    LIST OF FUNCTIONS
#  ********************************************************
def setup_logger(ch_config: dict, fh_config: dict):
    """
    Function to setup logging handlers to console or/and file with individual
    formatters and log levels. Option to rotate log files by time ('midnight').

    Args:
        ch_config (dict, required): StreamHandler to console. keys 'to_console', 'level'
        fh_config (dict, required): FileHandler or TimedRotatedFileHandler. keys
        'to_file', 'level', 'log_fn', 'file_mode', 'do_rotate', 'backup_cnt'
    """
    if ch_config["to_console"]:
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(ch_config["level"])
        ch_fmt = colorlog.ColoredFormatter(
            "%(name)-13s %(log_color)s%(levelname)-8s%(reset)s : %(message)s"
        )
        ch.setFormatter(ch_fmt)
        log.addHandler(ch)

    # file handler with option to rotate
    if fh_config["to_file"]:
        if fh_config["do_rotate"]:
            fh = TimedRotatingFileHandler(
                filename=fh_config["log_fn"],
                backupCount=fh_config["backup_cnt"],
                when="midnight",
            )
        else:
            fh = logging.FileHandler(
                filename=fh_config["log_fn"], mode=fh_config["file_mode"]
            )
        fh.setLevel(fh_config["level"])
        fh_fmt = logging.Formatter(
            "%(asctime)s %(name)-13s %(levelname)-8s : %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(fh_fmt)
        log.addHandler(fh)

    log.setLevel(logging.DEBUG)


def sd_notify(message):
    """
    Helper function for use of SystemdNotifier

    Args:
        message (str): message to be sent to SystemdNotifier
    """
    sd_notifier = sdnotify.SystemdNotifier()
    sd_notifier.notify(message)


def add_timestamp_to_message(message):
    return f'STATUS={strftime("%b %d %H:%M:%S", localtime())} - {unidecode(message)}'


def get_data_from_sma_energy_meter():
    """
    Capture broadcasting from SMA energy meter, decode message by calling
    the function decode_SMAEM and return content in a dictionary

    Returns:
        (dict): SMA energy meter decoded counter and actual values
    """
    #  --------------------------------------------------------------------
    #  create socket to listen to UDP broadcasting on MCAST_GRP, MCAST_PORT
    ipbind = "0.0.0.0"
    MCAST_GRP = "239.12.255.254"
    MCAST_PORT = 9522
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MCAST_PORT))
    try:
        mreq = struct.pack(
            "4s4s", socket.inet_aton(MCAST_GRP), socket.inet_aton(ipbind)
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        log.info("Successfully conncted to multicast group")
    except BaseException:
        log.error(
            "* SOCKET: could not connect to multicast group or bind to given interface"
        )
        sys.exit(1)
    smaeminfo = sock.recv(1024)
    # sock.shutdown()
    sock.close()
    sleep(0.3)
    return decode_SMAEM(smaeminfo)


#  MQTT callback and helper functions
def on_connect(client, userdata, flags, rc):
    """
    Callback function for CONNECTION event for the MQTT client
    """
    global mqtt_client_connected
    if rc == 0:
        mqtt_client_connected = True
        sd_notify(add_timestamp_to_message("MQTT connection established"))
        log.info("* MQTT connection establisehd")
        log.debug(f"  on_connect(): mqtt_client_connected = [{mqtt_client_connected}]")
    else:
        sd_notify(
            add_timestamp_to_message(
                f"MQTT connection error with result code {str(rc)} "
                + f"- {mqtt.connack_string(rc)}"
            )
        )
        log.error(
            f"* MQTT connection error with result code {str(rc)} "
            + f"- {mqtt.connack_string(rc)}"
        )
        mqtt_client_connected = False
        os._exit(1)


def on_disconnect(client, userdata, rc=0):
    """
    Callback function for DISCONNECT event for the MQTT client
    """
    sd_notify(add_timestamp_to_message("MQTT connection disconnected"))
    log.info("* Successfully disconnected from MQTT broker")


def on_publish(client, userdata, mid):
    """
    Callback function for PUBLISH event for the MQTT client
    """
    log.debug("  Data successfully published to MQTT broker")


def publish_to_mqtt(topic: str, payload: str, qos=0, retain=False):
    """
    MQTT helper function to publish data to the MQTT broker
    """
    log.debug(f"* Publishing to MQTT broker: topic: {topic}")
    log.debug(f"  data: {payload}")
    mqtt_client.publish(f"{topic}", payload=payload, qos=qos, retain=retain)
    sleep(0.5)


#  functions to handle MQTT alive timer
def publish_alive_status():
    """
    Helper function to send 'online' message to mqtt broker
    """
    log.debug("- publish alive message to MQTT broker ..")
    publish_to_mqtt(activity_topic, lwt_online_val)


def alive_timeout_handler():
    """
    Handler function for mqtt_alive_timer
    """
    log.debug("  interrupt mqtt_alive_timer ..")
    _thread.start_new_thread(publish_alive_status, ())
    start_alive_timer()


def start_alive_timer():
    """
    Helper function to manage alive timer (cancel expired threading Timer, create and
    start new one with length 'ALIVE_TIMEOUT_IN_SECONDS - default:  60'
    """
    global mqtt_alive_timer
    mqtt_alive_timer.cancel()
    log.debug("  mqtt_alive_timer stopped ..")
    mqtt_alive_timer = threading.Timer(ALIVE_TIMEOUT_IN_SECONDS, alive_timeout_handler)
    mqtt_alive_timer.start()
    log.debug("  new mqtt_alive_timer started ..")


#  functions to handle reporting timing and reporting
def reporting_handler():
    """
    Callback function for REPORTING Timer triggered by threading Timer after expiration
    """
    handle_interrupt(TIMER_INTERRUPT)
    start_reporting_timer()


def start_reporting_timer():
    """
    Helper function to manage reporting timer (cancel expired threading Timer, create
    and start new one with length 'reporting_interval_in_seconds'
    """
    global reporting_timer
    reporting_timer.cancel()
    reporting_timer = threading.Timer(reporting_interval_in_seconds, reporting_handler)
    reporting_timer.start()
    log.debug("  new reporting_timer started ..")


def handle_interrupt(channel: int):
    """
    Function to handle reporting event

    Args:
        channel (integer):
             0: reporting triggered by main program
            -1: reporting triggered by reporting timer
    """
    current_timestamp = datetime.now(local_tz)
    log.info(
        f"<<< INTR({channel}) >>> Time to report "
        + f'{current_timestamp.strftime("%H:%M:%S - %Y/%m/%d")}'
    )
    _thread.start_new_thread(send_status, (current_timestamp, ""))


def send_status(timestamp, _):
    """
    Function to send set of updated values from the SMA Energy Meter to the MQTT broker.

    Args:
        timestamp (timestamp): timestamp of reporting event
    """
    emdata = {}
    emdata = get_data_from_sma_energy_meter()
    sma_emdata = OrderedDict()
    sma_emdata["timestamp"] = timestamp.astimezone().replace(microsecond=0).isoformat()
    sma_emdata["grid_consume_total"] = emdata["p_consume_counter"]
    sma_emdata["grid_supply_total"] = emdata["p_supply_counter"]

    sma_em_top_dict = OrderedDict()
    sma_em_top_dict["info"] = sma_emdata

    _thread.start_new_thread(
        publish_to_mqtt, (values_topic, json.dumps(sma_em_top_dict), 1)
    )


#  ********************************************************
#             LOAD CONFIGURATION FILE config.ini
#  ********************************************************
config = ConfigParser(delimiters=("=",), inline_comment_prefixes=("#"))
config.optionxform = str
try:
    with open(os.path.join(config_dir, "config.ini")) as config_file:
        config.read_file(config_file)
except IOError:
    log.error(f'No configuration file "config.ini" found in directory {config_dir})')
    logging.shutdown()
    sys.exit(1)

#  read [LOG] section
fh_config["to_file"] = config["LOG"].getboolean("log_to_file_enabled", False)
default_log_filename = "sma-em.log"
fh_config["log_fn"] = config["LOG"].get("log_filename", default_log_filename)
default_file_mode = "a"
fh_config["file_mode"] = config["LOG"].get("file_mode", default_file_mode)
default_level_file = "WARNING"
fh_config["level"] = config["LOG"].get("file_log_level", default_level_file).upper()
fh_config["do_rotate"] = config["LOG"].getboolean("log_file_rotation", False)
default_backup_count = 6
fh_config["backup_cnt"] = config["LOG"].getint("backup_count", default_backup_count)

#  read [DAEMON] section
daemon_enabled = config["DAEMON"].getboolean("enabled", True)
min_interval_in_seconds = 20
max_interval_in_seconds = 300
default_reporting_interval_in_seconds = 60
reporting_interval_in_seconds = config["DAEMON"].getint(
    "reporting_interval_in_seconds", default_reporting_interval_in_seconds
)

#  read [MQTT] section
default_base_topic = "home/nodes"
base_topic = config["MQTT"].get("base_topic", default_base_topic).lower()

default_device_name = "smaem"
device_name = config["MQTT"].get("device_name", default_device_name).lower()

default_discovery_prefix = "homeassistant"
discovery_prefix = (
    config["MQTT"].get("discovery_previx", default_discovery_prefix).lower()
)

mqtt_hostname = os.environ.get(
    "MQTT_HOSTNAME", config["MQTT"].get("hostname", "localhost")
)
mqtt_port = int(os.environ.get("MQTT_PORT", config["MQTT"].get("port", "1883")))
mqtt_username = os.environ.get("MQTT_USERNAME", config["MQTT"].get("username"))
mqtt_password = os.environ.get("MQTT_PASSWROD", config["MQTT"].get("password", None))
mqtt_tls = config["MQTT"].getboolean("tls", False)
if mqtt_tls:
    ca_certs = config["MQTT"].get("tls_ca_cert", None)
    keyfile = config["MQTT"].get("tls_keyfile", None)
    certfile = config["MQTT"].get("tls_certfile", None)
    tls_version = ssl.PROTOCOL_SSLv23


#  check configuration
if (reporting_interval_in_seconds < min_interval_in_seconds) or (
    reporting_interval_in_seconds > max_interval_in_seconds
):
    log.error(
        'Invalid "reporting_interval_in_seconds" found in configuration file '
        + '"config.ini"! Value must be between '
        + f"[{min_interval_in_seconds} - {max_interval_in_seconds}]. "
        + "Fix it and try again ... aborting"
    )
    logging.shutdown()
    sys.exit(1)
if not config["MQTT"]:
    log.error(
        'No MQTT settings found in configuration file "config.ini'
        + "Fix it and try again .. aborting"
    )
    logging.shutdown()
    sys.exit(1)
if fh_config["to_file"] and fh_config["level"] not in [
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL",
]:
    log.error(
        "Logging level to file not recognized. Please verify and correct ... aborting"
    )
    logging.shutdown()
    sys.exit(1)


#  ********************************************************
#                       SETUP LOGGING
#  ********************************************************
setup_logger(ch_config=ch_config, fh_config=fh_config)
log.info(script_info)
log.info(project_name)
log.info("* Configuration accepted")


#  ********************************************************
#                   CONNECT TO MQTT BROKER
#  ********************************************************
log.info("* Connecting to MQTT broker ..")
mqtt_client_connected = False
mqtt_client = mqtt.Client()

#  connect callback functions to MQTT client
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish
mqtt_client.on_disconnect = on_disconnect

activity_topic = f"{base_topic}/{device_name.lower()}/status"
lwt_online_val = "online"
lwt_offline_val = "offline"

mqtt_client.will_set(activity_topic, payload=lwt_offline_val, retain=True)

if mqtt_username:
    mqtt_client.username_pw_set(mqtt_username, mqtt_password)

if mqtt_tls:
    mqtt_client.tls_set(
        ca_certs=ca_certs, certfile=certfile, keyfile=keyfile, tls_version=tls_version
    )

try:
    mqtt_client.connect(
        host=mqtt_hostname,
        port=mqtt_port,
        keepalive=ALIVE_TIMEOUT_IN_SECONDS,
    )
except ConnectionError:
    msg = (
        "MQTT connection error. Please check settings in the configuration "
        + 'file "config.ini"'
    )
    log.error(msg)
    sd_notify(add_timestamp_to_message(msg))
    logging.shutdown()
    sys.exit(1)
else:
    publish_to_mqtt(activity_topic, lwt_online_val)
    mqtt_client.loop_start()
    while not mqtt_client_connected:
        log.debug("  waiting to connect to MQTT broker ..")
        sleep(1.0)  # some slack to estabish the connection

# with MQTT connection established, notify systemd and launch the ALIVE time loop
sd_notify("READY=1")
mqtt_alive_timer = threading.Timer(ALIVE_TIMEOUT_IN_SECONDS, alive_timeout_handler)
start_alive_timer()


#  ********************************************************
#                   PERFORM MQTT DISCOVERY
#  ********************************************************
#  get unique serial number of the SMA Energy Meter to create uniqID
emdata = {}
emdata = get_data_from_sma_energy_meter()
serial = str(emdata["serial"])
uniqID = f"SMA-{serial[:5]}EM{serial[5:]}"
log.debug(f"uniqueID: {uniqID}")

#  SMA Energy Meter reporting device
LD_MONITOR = "monitor"
LD_ENERGY_CONSUME = "grid_consume_total"
LD_ENERGY_SUPPLY = "grid_supply_total"
LDS_PAYLOAD_NAME = "info"

#  dicionary of key items to publish:
detectorValues = OrderedDict(
    [
        (
            LD_MONITOR,
            dict(
                topic_category="sensor",
                title="SMA Energy Meter Monitor",
                device_class="timestamp",
                json_value="timestamp",
                json_attr="yes",
                icon="mdi:counter",
                device_ident=f"SMA-EM-{emdata['serial']}",
            ),
        ),
        (
            LD_ENERGY_CONSUME,
            dict(
                topic_category="sensor",
                title="Grid Consume",
                device_class="energy",
                state_class="total",
                json_value="grid_consume_total",
                unit="kWh",
                icon="mdi:counter",
            ),
        ),
        (
            LD_ENERGY_SUPPLY,
            dict(
                topic_category="sensor",
                title="Grid Supply",
                device_class="energy",
                state_class="total",
                json_value="grid_supply_total",
                unit="kWh",
                icon="mdi:counter",
            ),
        ),
    ]
)
log.info("* Announcing SMA Energy Meter to MQTT broker for auto-discovery")

values_topic_rel = f"~/{LD_MONITOR}"
values_topic = f"{base_topic}/sensor/{device_name.lower()}/{LD_MONITOR}"
activity_topic = f"{base_topic}/{device_name.lower()}/status"

log.debug(f"vaules topic rel: {values_topic_rel}")
log.debug(f"values topic: {values_topic}")
log.debug(f"activity topic: {activity_topic}")


for [sensor, params] in detectorValues.items():
    discovery_topic = (
        f"{discovery_prefix}/{params['topic_category']}/"
        + f"{device_name.lower()}/{sensor}/config"
    )
    sensor_base_topic = f"{base_topic}/{params['topic_category']}/{device_name.lower()}"

    payload = OrderedDict()
    payload["name"] = f"{params['title'].title()}"
    payload["uniq_id"] = f"{uniqID}_{sensor.lower()}"

    if "device_class" in params:
        payload["dev_cla"] = params["device_class"]

    if "state_class" in params:
        payload["stat_cla"] = params["state_class"]

    if "unit" in params:
        payload["unit_of_measurement"] = params["unit"]

    if "icon" in params:
        payload["ic"] = params["icon"]

    if "json_value" in params:
        payload["stat_t"] = values_topic_rel
        payload[
            "val_tpl"
        ] = f"{{{{ value_json.{LDS_PAYLOAD_NAME}.{params['json_value']} }}}}"

    payload["~"] = sensor_base_topic
    payload["avty_t"] = activity_topic
    payload["pl_avail"] = lwt_online_val
    payload["pl_not_avail"] = lwt_offline_val

    if "json_attr" in params:
        payload["json_attr_t"] = values_topic_rel
        payload["json_attr_tpl"] = "{{{{ value_json.{LDS_PAYLOAD_NAME} | tojson }}}}"
    if "device_ident" in params:
        payload["dev"] = {
            "identifiers": [f"{uniqID}"],
            "manufacturer": "SMA Solar Technology AG",
            "name": params["device_ident"],
            "model": "Energy Meter",
            "sw_version": f"{emdata['speedwire_version']}",
        }
    else:
        payload["dev"] = {"identifiers": [f"{uniqID}"]}

    publish_to_mqtt(discovery_topic, payload=json.dumps(payload), qos=1, retain=True)


#  ********************************************************
#                   LAUNCH REPORTING LOOP
#  ********************************************************
reporting_timer = threading.Timer(reporting_interval_in_seconds, reporting_handler)
start_reporting_timer()
sd_notify(add_timestamp_to_message("entered reporting loop"))
handle_interrupt(0)

#  now just hang in forever, until script is stopped externally
try:
    while True:
        sleep(10000)

#  cleanup and exit
finally:
    # stop mqtt client loop and disconnect
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    # cancel timers
    reporting_timer.cancel()
    mqtt_alive_timer.cancel()
    # shutdown logging
    logging.shutdown()
    log.info("* All timers canceled ... ready to exit!")
