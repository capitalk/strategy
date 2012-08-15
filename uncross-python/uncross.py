from proto_objs import spot_fx_md_1_pb2
from proto_objs import venue_configuration_pb2


import zmq 
import time 
import collections
from order_manager import BID, OFFER, OrderManager
import order_engine_constants
import uuid
import sys 

STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022'

STRATEGY_ID_BYTES = uuid.UUID(STRATEGY_ID).bytes 

Entry = collections.namedtuple('Entry', ('price', 'size', 'venue', 'symbol', 'timestamp'))

# a pair of entries for bid and offer
class Cross:
  def __init__(self, bid, offer):
    self.bid = bid
    self.offer = offer
    self.start_time = time.time()
    self.send_time = None
 
  
  def status_sent(self):
    self.sent_time = time.time()
  
  def __str__(self):
    return "Cross(bid = %s, offer = %s)" % (self.bid, self.offer)    
    
symbols_to_bids = {} 
# map each symbol (ie USD/JPY) to a dictionary from venue_id's to offer entries
symbols_to_offers = {}
# set of symbols whose data has been updated since the last time the function 
# 'find_best_crossed_pair' ran 
updated_symbols = set([])
context = zmq.Context()
current_cross = None
    
def update_market_data(msg):
  sys.stdout.write('.')
  sys.stdout.flush()

  timestamp = time.time()
  bbo = spot_fx_md_1_pb2.instrument_bbo();
  bbo.ParseFromString(msg)
  #print "Symbol", bbo.symbol
  #print "Venue", bbo.bid_venue_id
  symbol, venue_id = bbo.symbol, bbo.bid_venue_id  
  new_bid = Entry(bbo.bid_price, bbo.bid_size, venue_id, bbo.symbol, timestamp)  
  new_offer = Entry(bbo.ask_price, bbo.ask_size, venue_id, bbo.symbol, timestamp)
  
  # print "Bid", new_bid
  # print "Offer", new_offer
  # print   
  bids = symbols_to_bids.setdefault(symbol, {})
  old_bid = bids.get(venue_id)
  if old_bid != new_bid:
    updated_symbols.add(symbol)
    bids[venue_id] = new_bid
    
  offers = symbols_to_offers.setdefault(symbol, {})
  old_offer = offers.get(venue_id)
  if old_offer != new_offer:
    updated_symbols.add(symbol)
    offers[venue_id] = new_offer
  


def find_best_crossed_pair(min_cross_magnitude, max_size = 100000000):
  assert current_cross is None
  if len(updated_symbols) == 0: return
  # print "UPDATED SYMBOLS", updated_symbols
  best_cross = None
  best_cross_magnitude = 0
  for symbol in updated_symbols:
     
    bid_venues =  symbols_to_bids[symbol]
    yen_pair = "JPY" in symbol
    
    offer_venues = symbols_to_offers[symbol]
    # bids sorted from highest to lowest 
    sorted_bids = sorted(bid_venues.items(), key=lambda (v,e): e.price, reverse=True)
    # offers from lowest to highest
    sorted_offers = sorted(offer_venues.items(), key=lambda (v,e): e.price)
    for (bid_venue, bid_entry) in sorted_bids:
      for (offer_venue, offer_entry) in sorted_offers:
        price_difference = bid_entry.price - offer_entry.price
        if price_difference < 0: 
          break
        else:
          cross_size = min(bid_entry.size, offer_entry.size)
          cross_magnitude = price_difference * cross_size
          print "--- ", symbol, cross_size, cross_magnitude
          if yen_pair: cross_magnitude /= 80
          if cross_magnitude > best_cross_magnitude:
            best_cross = Cross(bid = bid_entry, offer = offer_entry)
            best_cross_magnitude = cross_magnitude 
            print 
            print "-- Found better cross: ", best_cross
  if best_cross is not None:
    print 
    print "BEST CROSS:", best_cross 
    print 
    if best_cross_magnitude < min_cross_magnitude: 
      best_cross = None
  updated_symbols.clear()
  return best_cross
     


def synchronize_market_data(market_data_socket, wait_time):
  print "Synchronizing market data"
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  start_time = time.time()
  while time.time() < start_time + wait_time:
    ready_sockets = dict(poller.poll(1000))
    if ready_sockets.get(market_data_socket) == zmq.POLLIN:
      update_market_data(market_data_socket.recv())
  print "Waited", wait_time, "seconds, entering main loop"

def outgoing_logic(om, min_cross_magnitude = 50, new_order_delay = 0,  max_order_lifetime = 5):
  curr_time = time.time()
  global current_cross 
  
  if current_cross is None:
    current_cross = find_best_crossed_pair(min_cross_magnitude)
  
  # these if statements look repetitve but remember that find_best_crossed_pair
  # might return None if there is no good cross
  if current_cross is not None:
    sys.stdout.write('O')
    sys.stdout.flush()
    bid = current_cross.bid
    offer = current_cross.offer
    
    if current_cross.send_time is None and (current_cross.start_time + new_order_delay >= curr_time):
      # send the damn thing 
      print "Sending orders for %s" % current_cross
      om.send_new_order(bid.venue, bid.symbol, BID, bid.price, bid.size)
      om.send_new_order(offer.venue, offer.symbol, OFFER, offer.price, offer.size)
      current_cross.status_sent()
      
    # if we've already sent an order, check if it's expired
    elif current_cross.send_time is not None:
      expired = curr_time >= current_cross.send_time + max_order_lifetime 
      n_open = len(om.live_order_ids)
      if n_open == 0:
        current_cross = None
      elif n_open == 1 and expired:
        om.liquidate_immediately(om.live_order_ids.pop())
      elif n_open == 2 and expired:
        om.cancel_everything()
      else:
        raise RuntimeError("Didn't expect to have %d open orders simultaneously" % n_open)
        
def main_loop(market_data_socket, order_sockets, min_cross_magnitude = 50, new_order_delay = 0, max_order_lifetime = 5):
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  for order_socket in order_sockets.values():
    poller.register(order_socket, zmq.POLLIN)
  
  om = OrderManager(STRATEGY_ID_BYTES, order_sockets)
  while True:
    ready_sockets = poller.poll()
    for (socket, state) in ready_sockets:
      # ignore errors for now
      if state == zmq.POLLERR:
        print "POLLERR on socket", socket, "md socket = ", market_data_socket, "order sockets = ", order_sockets 
        #print msg 
      elif state == zmq.POLLIN:
        if socket == market_data_socket:
          msg = market_data_socket.recv()
          update_market_data(msg)
        else:
          [tag, msg] = socket.recv_multipart()
          tag = int_from_bytes(tag) 
          om.received_message_from_order_engine(tag, msg)
    outgoing_logic(om, min_cross_magnitude, new_order_delay, max_order_lifetime)

def poll_single_socket(socket, timeout= 1.0):
  msg_parts = None
  for i in range(10):
   
    time.sleep(timeout / 10.0)
    try:
      msg_parts = socket.recv_multipart(zmq.NOBLOCK)
    except:
      pass 
    if msg_parts: return msg_parts
    else:
      if i == 0:
        sys.stdout.write('Waiting for socket...')
        sys.stdout.flush()
      else:
        sys.stdout.write(".")
        sys.stdout.flush()
  print 
  return None


hello_tag = chr( order_engine_constants.STRATEGY_HELO)

import struct
def int_from_bytes(bytes):
  assert len(bytes) == 4
  return struct.unpack('<I', bytes)[0]

def say_hello(socket):
  socket.send_multipart([hello_tag, STRATEGY_ID_BYTES])
  message_parts = poll_single_socket(socket, 3)
  if message_parts:
    [tag, venue_id] = message_parts
    tag = int_from_bytes(tag)
    venue_id = int_from_bytes(venue_id)
    assert tag == order_engine_constants.STRATEGY_HELO_ACK, "Unexpected response to HELO: %d" % tag 
    return venue_id
  else:
    raise RuntimeError("Didn't get response to HELO from order engine")

def connect_to_order_engine(addr):
  order_socket = context.socket(zmq.XREQ) #zmq.DEALER)
  print "Connecting order engine socket to", addr
  order_socket.connect(addr)
  venue_id = say_hello(order_socket)
  if not venue_id:
    raise RuntimeError("Couldn't say HELO to order engine at " + addr)
  print "...got venue_id =", venue_id
  return order_socket, venue_id 

def ping(socket, name = None):
  t0 = time.time()
  socket.send(chr(order_engine_constants.PING))
  message_parts = poll_single_socket(socket, 1)
  if message_parts: 
    tag = int_from_bytes(message_parts[0])
    tag == order_engine_constants.PING_ACK
    return time.time() - t0 
  else:
    if not name: name = "<not given>"
    raise RuntimeError("Timed out waiting for ping ack from %s" % name)
  
def connect_to_order_engine_controller(addr):
  assert isinstance(addr, str) 
  print "Connecting control socket to %s" %  addr 
  order_control_socket = context.socket(zmq.REQ)
  try:
    order_control_socket.connect(addr)
    #order_control_socket.connect('tcp://127.0.0.1:7998')
    ping(order_control_socket)
    return order_control_socket 
  except:
    print "Failed to ping", addr
    print 
    return  None

def address_ok(addr):
  if not isinstance(addr, str):
    print "%s must be a string, not %s " % (addr,  type(addr))
    return False
  if len(addr) <= 3:
    print "%s too short" % addr
    return False
  if addr[:3] not in ['tcp', 'ipc']:
    print "Unknown protocol: %s" % addr[:3]
    return False
  return True 
 
def init(config_server_addr, symbols = None):
  config_socket = context.socket(zmq.REQ)
  config_socket.connect(config_server_addr)
  print "Requesting configuation"
  config_socket.send('C')
  [tag, msg] = config_socket.recv_multipart()
  assert tag == "CONFIG"
  config = venue_configuration_pb2.configuration()
  config.ParseFromString(msg)
  
  md_socket = context.socket(zmq.SUB)
  order_sockets = {}
  order_control_sockets = {}
  mic_names = {}
  for venue_config in config.configs:
    # convert everything to str manually since zmq hates unicode
    venue_id = int(venue_config.venue_id)
    mic_name = str(venue_config.mic_name)
    ping_addr = str(venue_config.order_ping_addr)
    order_addr = str(venue_config.order_interface_addr)
    md_addr = str(venue_config.market_data_broadcast_addr)
    print "Reading config for mic = %s, venue_id = %s" % (mic_name, venue_id)
    if address_ok(ping_addr) and address_ok(order_addr) and address_ok(md_addr): 
      order_control_socket = connect_to_order_engine_controller(ping_addr)
      if order_control_socket:
        print "Ping succeeded, adding sockets..."
        order_socket, venue_id2 = connect_to_order_engine (order_addr)
        assert venue_id == venue_id2, "%s != %s (types %s, %s)" % \
          (venue_id, venue_id2, type(venue_id), type(venue_id2))
        order_sockets[venue_id] = order_socket
        mic_names[venue_id] = mic_name
        order_control_sockets[venue_id] = order_control_socket
        md_socket.connect(md_addr)
        print "Succeeded in connecting to %s" % mic_name
        print 
  if symbols is None:
    md_socket.setsockopt(zmq.SUBSCRIBE, "")
  else:
    for s in symbols:
       md_socket.setsockopt(zmq.SUBSCRIBE, s)
  print "--------------------------------------"
  print "Active markets:",  ", ".join(mic_names.values())
  print "--------------------------------------"
  print 
  return md_socket, order_sockets, order_control_sockets, mic_names
 

from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
#parser.add_argument('--market-data', type=str, nargs='*', default=[],  dest = 'md_addrs')
#parser.add_argument('--order-engine', type=str, nargs='*', default = [], dest='order_engine_addrs')
parser.add_argument('--max-order-size', type=int, default=10000000, dest='max_order_size')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")
parser.add_argument('--min-cross-magnitude', type=float, default = 50, dest = 'min_cross_magnitude')
parser.add_argument('--max-order-lifetime', type=float, default = 5.0, dest='max_order_lifetime')

def cleanup(sockets):
  print "Running cleanup code" 
  for socket in sockets:
    socket.setsockopt(zmq.LINGER, 0)
    socket.close()

import atexit  
if __name__ == '__main__':
  args = parser.parse_args()
  #assert len(args.md_addrs) > 0
  md_socket, order_sockets, order_control_sockets, _ = init(args.config_server)
  all_sockets = [md_socket] + order_sockets.values() + order_control_sockets.values()
  atexit.register(lambda: cleanup(all_sockets))
  synchronize_market_data(md_socket, args.startup_wait_time)
  main_loop(md_socket, order_sockets, args.min_cross_magnitude, args.order_delay, args.max_order_lifetime)
  
    
