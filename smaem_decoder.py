#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
"""
Functions to decode the udp telegram message sent out by the 
SMA Energy Meter on port 9522 of the multicast group 239.12.255.254
 
Documentation of the protocol is unfortunately only available in German. It 
can be downloaded from:
    https://github.com/ufankhau/sma-empv/documentation/SMA-EM_GE.pdf

The core of the following code is taken from the work of david-m-m and
datenschuft (https://github.com/datenschuft/SMA-EM) and adjusted to fit
my needs
"""

import logging

log = logging.getLogger(__name__)


#  map of all SMA-EM measurement channels in the sma_channels dictionary
#
#  <index>:(<smaem_name>,<unit_actual_value>,<unit_counter_value>)
sma_channels = {
    # totals
    1: ("p_consume", "W", "kWh"),
    2: ("p_supply", "W", "kWh"),
    3: ("q_consume", "VAr", "kVArh"),
    4: ("q_supply", "VAr", "kVArh"),
    9: ("s_consume", "VA", "kVAh"),
    10: ("s_supply", "VA", "kVAh"),
    13: ("cosphi", ""),
    14: ("freq", "Hz"),  # firmware 2.xxxx and higher
    # phase 1
    21: ("p1_consume", "W", "kWh"),
    22: ("p1_supply", "W", "kWh"),
    23: ("q1_consume", "VAr", "kVArh"),
    24: ("q1_supply", "VAr", "kVArh"),
    29: ("s1_consume", "VA", "kVAh"),
    30: ("s1_supply", "VA", "kVAh"),
    31: ("i1", "A"),
    32: ("u1", "V"),
    33: ("cosphi1", ""),
    # phase 2
    41: ("p2_consume", "W", "kWh"),
    42: ("p2_supply", "W", "kWh"),
    43: ("q2_consume", "VAr", "kVArh"),
    44: ("q2_supply", "VAr", "kVArh"),
    49: ("s2_consume", "VA", "kVAh"),
    50: ("s2_supply", "VA", "kVAh"),
    51: ("i2", "A"),
    52: ("u2", "V"),
    53: ("cosphi2", ""),
    # phase 3
    61: ("p3_consume", "W", "kWh"),
    62: ("p3_supply", "W", "kWh"),
    63: ("q3_consume", "VAr", "kVArh"),
    64: ("q3_supply", "VAr", "kVArh"),
    69: ("s3_consume", "VA", "kVAh"),
    70: ("s3_supply", "VA", "kVAh"),
    71: ("i3", "A"),
    72: ("u3", "V"),
    73: ("cosphi3", ""),
    # other
    0: ("speedwire_version", ""),
}


"""
SMA sends their data ("value" or "counter") in the following units

    power in		  0.1 W
    energy in		    1 Ws
    current in		    1 mA
    voltage in		    1 mV
    frequency in		0.001 Hz
    power factor in	0.001 of cos(phi)

This results in the following dictionary "sma_scale" to get to the units
specified in the "sma_channels" dictionary
"""
sma_scale = {
    "W": 10,
    "VA": 10,
    "VAr": 10,
    "kWh": 3600000,
    "kVAh": 3600000,
    "kVArh": 3600000,
    "A": 1000,
    "V": 1000,
    "Hz": 1000,
    "": 1000,
}


def decode_OBIS(obis: bytes):
    """
    Decoder for 4 bytes OBIS identifier (for details see SMA documentation):
        byte 0: 1 for electricty, code 144 used by SMA for sending software version
        byte 1: index according to OBIS channel definition
        byte 2: type of measurement, 2 types are in use, 'actual' value and 'counter'
        byte 3: not used

    Args:
        obis (bytes): OBIS identifier

    Returns:
        tuple (int, string):  sma channel index, datatype
        ('actual', 'counter', 'version', 'unknown')
    """
    obis_channel = obis[0]
    obis_index = obis[1]
    obis_type = obis[2]
    if obis_type == 4:
        datatype = "actual"
    elif obis_type == 8:
        datatype = "counter"
    elif obis_type == 0 and obis_channel == 144:
        datatype = "version"
    else:
        datatype = "unknown"
        log.warning(
            f"OBIS: unknown datatype: channel: {obis_channel} "
            + f"index: {obis_index} type: {obis_type}"
        )
    return (obis_index, datatype)


def decode_SMAEM(datagram: bytes):
    """
    Decoder for SMA Energy Meter broadcasted messenges.

    Args:
        datagram (bytes): encoded data broadcasted by SMA Energy Meter

    Returns:
        dictionary: values and counters (sum and per phase)
    """
    em_data = {}

    # process data only if SMA header is present
    if datagram[0:3] == b"SMA":
        # datagram length
        datalength = int.from_bytes(datagram[12:14], byteorder="big") + 16

        if datalength != 54:
            # serial number of engery meter
            emID = int.from_bytes(datagram[20:24], byteorder="big")
            em_data["serial"] = emID

            # timestamp of em message
            timestamp = int.from_bytes(datagram[24:28], byteorder="big")
            em_data["timestamp"] = timestamp

            # starting with position 28, loop over remaining length of "datagram"
            # and decode OBIS data blocks
            position = 28
            while position < datalength:
                # decode header
                (obis_index, datatype) = decode_OBIS(datagram[position : position + 4])

                if datatype == "actual":
                    value = int.from_bytes(
                        datagram[position + 4 : position + 8], byteorder="big"
                    )
                    position += 8
                    if obis_index in sma_channels.keys():
                        em_data[sma_channels[obis_index][0]] = (
                            value / sma_scale[sma_channels[obis_index][1]]
                        )
                        em_data[sma_channels[obis_index][0] + "_unit"] = sma_channels[
                            obis_index
                        ][1]

                elif datatype == "counter":
                    value = int.from_bytes(
                        datagram[position + 4 : position + 12], byteorder="big"
                    )
                    position += 12
                    if obis_index in sma_channels.keys():
                        em_data[sma_channels[obis_index][0] + "_counter"] = (
                            value / sma_scale[sma_channels[obis_index][2]]
                        )
                        em_data[
                            sma_channels[obis_index][0] + "_counterunit"
                        ] = sma_channels[obis_index][2]

                elif datatype == "version":
                    if obis_index in sma_channels.keys():
                        byte3 = datagram[position + 4]
                        byte2 = datagram[position + 5]
                        byte1 = datagram[position + 6]
                        byte0 = datagram[position + 7]
                        version = (
                            str(byte3)
                            + "."
                            + str(byte2).zfill(2)
                            + "."
                            + str(byte1).zfill(2)
                            + "."
                            + str(chr(byte0))
                        )
                        em_data[sma_channels[obis_index][0]] = version
                    position += 8
                else:
                    position += 8

    log.debug("* Decode SMAEM")
    for key in em_data.keys():
        log.debug(f"  {key}: {em_data[key]}")

    return em_data
