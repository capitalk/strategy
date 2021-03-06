#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid
import zmq
import time
import logging
import sys

from proto_objs import spot_fx_md_1_pb2
from proto_objs import venue_configuration_pb2
from int_util import int_to_bytes, int_from_bytes
from order_manager2 import OrderManager
import order_engine_constants


import venue_attrs
from venue_attrs import venue_capabilities
#venue_specifics = {}
#
#
#class venue_capabilities:
#
#    def __init__(self, venue_id):
#        self.venue_id = venue_id
#        self.use_synthetic_cancel_replace = False
#
#    def use_synthetic_cancel_replace(self):
#        return self.use_synthetic_cancel_replace
#

context = zmq.Context()


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


hello_tag = int_to_bytes(order_engine_constants.STRATEGY_HELO)


def say_hello(socket, strategy_id_bytes):
    socket.send_multipart([hello_tag, strategy_id_bytes])
    message_parts = poll_single_socket(socket, 1)
    if message_parts:
        [tag, venue_id] = message_parts
        tag = int_from_bytes(tag)
        venue_id = int_from_bytes(venue_id)
        assert tag == order_engine_constants.STRATEGY_HELO_ACK, \
            'Unexpected response to HELO: %d' % tag
        return venue_id
    else:
        raise RuntimeError("Didn't get response to HELO from order engine"
                           )


def connect_to_order_engine(addr, strategy_id_bytes, mic_name):
    order_socket = context.socket(zmq.DEALER)
    print 'Connecting order engine socket to', addr
    order_socket.connect(addr)
    venue_id = say_hello(order_socket, strategy_id_bytes)
    if not venue_id:
        raise RuntimeError("Couldn't say HELO to order engine at "
                           + addr)
    print '...got venue_id =', venue_id
    return (order_socket, venue_id)


def ping(socket, name=None):
    t0 = time.time()
    socket.send(int_to_bytes(order_engine_constants.PING))
    message_parts = poll_single_socket(socket, 0.25)
    if message_parts:
        tag = int_from_bytes(message_parts[0])
        tag == order_engine_constants.PING_ACK
        return time.time() - t0
    else:
        if not name:
            name = '<not given>'
        raise RuntimeError('Timed out waiting for ping ack from %s'
                           % name)


def connect_to_order_engine_controller(addr):
    order_control_socket = context.socket(zmq.REQ)
    print 'Connecting control socket to %s' % addr
    order_control_socket.connect(addr)
    try:
        ping(order_control_socket)
        return order_control_socket
    except:
        print 'Ping failed'
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


class Strategy:

    def __init__(self, strategy_id, symbols=None):
        self.strategy_id = uuid.UUID(strategy_id)
        self.strategy_id_bytes = self.strategy_id.bytes

    # market data socket

        self.md_socket = context.socket(zmq.SUB)

    # map from venue_id to order socket

        self.order_sockets = {}

    # map from venue_id to order ping/control socket

        self.order_control_sockets = {}

    # map from venue_id to MIC name of venue

        self.mic_names = {}

    # which symbols should MD sockets listen for?

        self.symbols = None

        self.config_socket = context.socket(zmq.REQ)

        self.order_manager = None

    def connect(self, config_server_addr, verbose=True):
        """Talk to the config server and get addresses for 
       all available order engines and market data feeds, 
       return an order manager which is connected to all order sockets
    """

        config_socket = self.config_socket
        print 'Requesting configuation from', config_server_addr
        config_socket.connect(config_server_addr)
        config_socket.send('C')
        response = poll_single_socket(config_socket)
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

            problem_with_addr = False
            for addr in [ping_addr, order_addr, md_addr]:
                if not address_ok(addr):
                    print 'Malformed address', addr
                    problem_with_addr = True
            if problem_with_addr:
                print 'Skipping', mic_name
                continue
            order_control_socket = \
                connect_to_order_engine_controller(ping_addr)
            if order_control_socket:
                print 'Ping succeeded, adding sockets...'
                (order_socket, venue_id2) = \
                    connect_to_order_engine(order_addr,
                        self.strategy_id_bytes, mic_name)
                assert venue_id == venue_id2
                self.order_sockets[venue_id] = order_socket
                self.mic_names[venue_id] = mic_name
                self.order_control_sockets[venue_id] = \
                    order_control_socket
                self.md_socket.connect(md_addr)
        if self.symbols is None:
            self.md_socket.setsockopt(zmq.SUBSCRIBE, '')
        else:
            for s in self.symbols:
                self.md_socket.setsockopt(zmq.SUBSCRIBE, s)
        names = self.mic_names.values()
        if len(names) > 0:
            print
            print '----------------------------'
            print 'Succeeded in connecting to: ', ', '.join(names)
            print '----------------------------'
            print
        else:
            raise RuntimeError("Couldn't find any active venues")

    # return the set of valid venue_ids

        self.order_manager = OrderManager(self.strategy_id_bytes,
                self.order_sockets)
        return self.order_manager

    def close_all(self):
        print 'Running cleanup code'
        sockets = [self.md_socket] + self.order_sockets.values() \
            + self.order_control_sockets.values()
        for socket in sockets:
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    def _synchronize_market_data(self, md_update, wait_time=0.5):
        """For a short period only receive market data without taking any
       actions. Every time a new piece of market data arrives, 
       parse it and call 'md_update' with the parsed struct
    """

        logging.debug('Synchronizing market data')
        poller = zmq.Poller()
        poller.register(self.md_socket, zmq.POLLIN)
        start_time = time.time()
        while time.time() < start_time + wait_time:
            ready_sockets = dict(poller.poll(1000))
            if ready_sockets.get(self.md_socket) == zmq.POLLIN:
                bbo = spot_fx_md_1_pb2.instrument_bbo()
                msg = self.md_socket.recv()
                bbo.ParseFromString(msg)
                md_update(bbo)
        print 'Waited', wait_time, 'seconds, entering main loop'

    def run(self, md_update, place_orders, order_first=False):
        self._synchronize_market_data(md_update)
        poller = zmq.Poller()
        md_socket = self.md_socket
        poller.register(md_socket, zmq.POLLIN)
        print 'ORDER SOCKETS:', self.order_sockets
        print 'ORDER SOCKET VALUES: ', self.order_sockets.values()
        for order_socket in self.order_sockets.values():
            poller.register(order_socket, zmq.POLLIN)

        while True:
            if order_first == True:
                place_orders()
            ready_sockets = poller.poll()
            for (socket, state) in ready_sockets:
        # ignore errors for now
                if state == zmq.POLLERR:
                    print 'POLLERR on socket', socket, 'md socket = ', \
                        self.md_socket, 'order sockets = ', \
                        self.order_sockets
                elif state == zmq.POLLIN:
                    if socket == md_socket:
                        msg = md_socket.recv()
                        bbo = spot_fx_md_1_pb2.instrument_bbo()
                        msg = md_socket.recv()
                        bbo.ParseFromString(msg)
                        md_update(bbo)
                    else:
                        [tag, msg] = socket.recv_multipart()
                        tag = int_from_bytes(tag)
                        self.order_manager.received_message_from_order_engine(tag,
                                msg)
            place_orders()


