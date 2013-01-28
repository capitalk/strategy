#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid
import zmq
import time
import logging
import sys
import venue_attrs

from proto_objs import venue_configuration_pb2
from venue_attrs import venue_capabilities


def poll_single_socket(socket, timeout=1.0):
    msg_parts = None
    for i in range(10):
        time.sleep(timeout / 10.0)
        try:
            msg_parts = socket.recv_multipart(zmq.NOBLOCK)
        except:
            pass
        if msg_parts:
            return msg_parts
        else:
            if i == 0:
                sys.stdout.write('Waiting for socket...')
                sys.stdout.flush()
            else:
                sys.stdout.write('.')
                sys.stdout.flush()
    return None



def address_ok(addr):
    type_ok = isinstance(addr, str)
    try:
        prefix = addr[:3]
        prefix_ok = prefix in ['tcp', 'ipc']
        port = addr.split(':')[-1]
        port_ok = int(port) != 0
        return prefix_ok and type_ok and port_ok
    except:
        return False

class Configuration:
    def __init__(self, context):
        self.config_socket = context.socket(zmq.REQ)
        self.config_socket.setsockopt(zmq.LINGER, 0)

    def connect(self, config_server_addr, verbose=True):
        """ Get config information from config server at default
        address or address specified on command line as override"""

        self.config_socket = self.config_socket

        print 'Connecting to configuration server:', config_server_addr
        self.config_socket.connect(config_server_addr)

    def get_configs(self):
        self.config_socket.send('C')
        response = poll_single_socket(self.config_socket)
        if response is None:
            raise RuntimeError('Config server is down')
        [tag, msg] = response
        assert tag == 'CONFIG'
        config = venue_configuration_pb2.configuration()
        config.ParseFromString(msg)

        for venue_config in config.configs:
            venue_id = int(venue_config.venue_id)
            mic_name = str(venue_config.mic_name)
            print
            print 'Reading config for mic = %s, venue_id = %s' \
                % (mic_name, venue_id)

            ping_addr = str(venue_config.order_ping_addr)
            order_addr = str(venue_config.order_interface_addr)
            md_addr = str(venue_config.market_data_broadcast_addr)
            log_addr = str(venue_config.logging_broadcast_addr)

            vc = venue_capabilities(venue_id)
            venue_attrs.venue_specifics[venue_id] = vc
            if venue_config.use_synthetic_cancel_replace is True:
                print 'Setting synthetic_cancel_replace for: %d' % venue_id
                venue_attrs.venue_specifics[venue_id].use_synthetic_cancel_replace = True

        return config

    def refresh_config(self):
        self.config_socket.send('R')
        response = poll_single_socket(self.config_socket)
        if response is None:
            raise RuntimeError('Config server is down')


