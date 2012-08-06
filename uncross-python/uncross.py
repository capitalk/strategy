from proto_objs import spot_fx_md_1_pb2
import gevent 
import zmq 
import time 
import collections
from fix_constants import ORDER_STATUS, EXEC_TYPE
import order_engine_constants
from enum import enum 



STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022'

Entry = collections.namedtuple('Entry', ('price', 'size', 'venue', 'symbol', 'timestamp'))
# a pair of entries for bid and offer
class Cross:
  def __init__(self, bid, offer):
    self.bid = bid
    self.offer = offer
    self.start_time = time.time()
    self.send_time = None
    self.bid_id = None
    self.offer_id = None
  
  def sent(self, bid_id, offer_id):
    self.bid_id = bid_id
    self.offer_id = offer_id
    self.sent_time = time.time()
    
symbols_to_bids = {} 
# map each symbol (ie USD/JPY) to a dictionary from venue_id's to offer entries
symbols_to_offers = {}
# set of symbols whose data has been updated since the last time the function 
# 'find_best_crossed_pair' ran 
updated_symbols = set([])

context = zmq.Context()

current_cross = None
    
def update_market_data(bbo):
  timestamp = time.time()
  print "Symbol", bbo.symbol
  print "Venue", bbo.bb_venue_id
  symbol, venue_id = bbo.symbol, bbo.bb_venue_id  
  new_bid = Entry(bbo.bb_price, bbo.bb_size, venue_id, bbo.symbol, timestamp)  
  new_offer = Entry(bbo.ba_price, bbo.ba_size, venue_id, bbo.symbol, timestamp)
  
  print "Bid", new_bid
  print "Offer", new_offer
    
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
  


def find_best_crossed_pair(max_size = 10000000, min_cross_magnitude = 50):
  assert current_cross is None
  if len(updated_symbols) == 0: return
  print "UPDATED SYMBOLS", updated_symbols
  updated_symbols.clear()
  best_cross = None
  best_cross_magnitude = min_cross_magnitude
  for (symbol, bid_venues) in symbols_to_bids.iteritems():
    yen_pair = "JPY" in symbol
    offer_venues = symbols_to_offers[symbol]
    # bids sorted from highest to lowest 
    sorted_bids = sorted(bid_venues, key=lambda (v,e): e.price, reverse=True)
    # offers from lowest to highest
    sorted_offers = sorted(offer_venues, key=lambda (v,e): e.price)
    for (bid_venue, bid_entry) in sorted_bids:
      for (offer_venue, offer_entry) in sorted_offers:
        if bid_entry.price <= offer_entry.price: break
        else:
          cross_size = min(bid_entry.size, offer_entry.size)
          price_difference = bid_entry.price - offer_entry.price
          cross_magnitude = price_difference * cross_size
          if yen_pair: cross_magnitude /= 80
          if cross_magnitude > best_cross_magnitude:
            best_cross = Cross(bid = bid_entry, offer = offer_entry)
            best_cross_magnitude = cross_magnitude 
            print "Found better cross: ", best_cross
  return best_cross
     

def say_hello(order_control_socket):
  socket.send_multipart([order_engine_constants.STRATEGY_HELO, STRATEGY_ID])
  [tag, venue_id] = socket.recv_multipart()
  assert tag == order_engine_constants.STRATEGY_HELO_ACK
  return venue_id

def synchronize_market_data(market_data_socket, wait_time):
  print "Synchronizing market data"
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  start_time = time.time()
  while time.time() < start_time + wait_time:
    ready_sockets = dict(poller.poll(1000))
    if ready_sockets.get(market_data_socket) == zmq.POLLIN:
      msg = market_data_socket.recv()
      receive_market_data(msg)
  print "Waited", wait_time, "seconds, entering main loop"

def outgoing_logic(order_socket, order_manager, new_order_delay = 0,  max_order_lifetime = 5):
  curr_time = time.time()
  if current_cross is None:
    current_cross = find_best_crossed_pair()
    
  # if we've identified a cross but haven't sent it yet 
  # (and we're past the delay period) then send it to the order engine
  if current_cross is not None:
    bid = current_cross.bid
    offer = current_cross.offer
    
    if cross.send_time is None and (cross.start_time + new_order_delay >= curr_time):
      print "Sending orders for %s" % current_cross
      bid_pb = order_manager.make_new_order(bid.venue, bid.symbol, order_manager.BID, bid.price, bid.size)
      order_engine.send_multipart([order_engine_constants.ORDER_NEW, bid_pb])
      offer_pb = order_manager.make_new_order(offer.venue, offer.symbol, order_manager.OFFER, offer.price, offer.size)
      order_engine.send_multipart([order_engine_constants.ORDER_NEW, offer_pb])
      current_cross.sent(bid_pb.cl_order_id, offer_pb.cl_order_id)
      
    # if we've already sent an order, check if it's expired
    elif current_cross.send_time is not None:
      bid_still_alive = current_cross.bid_id in order_manager.live_order_ids
      offer_still_alive = current_cross.offer_id in order_manager.live_order_ids
      expired = current_cross.send_time + max_order_lifetime >= curr_time
      if bid_still_alive and offer_still_alive and expired:
        # if both orders still alive and expired, cancel them both
      elif bid_still_alive and expired:
        # bid is alive and expired, cancel/replace it to a shitty price
        # to make sure we get a fill 
      elif offer_still_alive and expired:
        # offer is still alive and expired, cancel/replace it to a shitty 
        # price to make sure we get a fill
      else:
        # both orders are gone! 
        current_cross = None
        
def main_loop(market_data_socket, order_socket, new_order_delay = 0, max_order_lifetime = 5):
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  poller.register(order_socket,  zmq.POLLIN|zmq.POLLOUT)
  while True:
    ready_sockets = dict(poller.poll(1000))    
    if ready_sockets.get(market_data_socket) == zmq.POLLIN:
      print "POLLIN: market data"
      msg = market_data_socket.recv()
      bbo = spot_fx_md_1_pb2.instrument_bbo();
      bbo.ParseFromString(msg)
      update_market_data(bbo)
    
    if ready_sockets.get(order_socket) == zmq.POLLIN:
      print "POLLIN: order engine"
      [tag, msg] = order_socket.recv_multipart()
      order_manager.received_message_from_order_engine(tag, msg)
      
    elif ready_sockets.get(order_socket) == zmq.POLLOUT:
      print "POLLOUT: order engine"
      outgoing_logic(order_socket, order_manager, new_order_delay, max_order_lifetime)
            
          
def init(args):
  md_socket = context.socket(zmq.SUB)
  #for addr in args.md_addrs:
  #   socket.connect(addr)
  #  md_target = "%s:%s" % (args.md_addrs)
  #  print "Making socket for", target
  print "Connecting Market Data to %s" % args.md_addr
  md_socket.connect(args.md_addr)
  
  if symbols is None:
    print "Subscribing to all messages" 
    socket.setsockopt(zmq.SUBSCRIBE, "")
  else:
    raise RuntimeError("Removed support for selective subscriptions!")

  if args.order_engine_addr and args.order_engine_control_addr:
    # the order socket is the one through which we send orders and
    # receive execution reports 
    order_socket = context.socket(zmq.DEALER)
    print "Connecting Order Engine socket to", args.order_engine_addr
    order_socket.connect(args.order_engine_addr)
    
    order_control_socket = context.socket(zmq.REQ)
    print "Connecting Order Engine Control socket to %s" % args.order_engine_control_addr
    order_control_socket.connect(args.order_engine_control_addr)
    
    got_ack = say_hello(order_control_socket)
    if not got_ack:
      raise RuntimeError("Couldn't connect to order engine")
  elif args.order_engine_addr or args.order_engine_control_addr:
    print "Warning: If you give an order engine port you must also give an order engine control port"
    order_socket = None
    order_control_socket = None
  else:
    print "No order engine addresses given, only subscribing to market data"
    order_socket = None
    order_control_socket = None
  return md_socket, order_socket, order_control_socket
  
    

from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--market-data', type=str, required=True, dest = 'md_addr')
parser.add_argument('--order-engine', type=str, default= None, dest='order_engine_addr')
parser.add_argument('--order-engine-control', type=str, default=None, dest='order_engine_control_addr')
parser.add_argument('--max-order-size', type=int, default=10000000, dest='max_order_size')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")

  

if __name__ == '__main__':
  args = parser.parse_args()
  md_socket, order_socket, order_control_socket = init(args)
  synchronize_with_market(market_data_socket, args.startup_wait_time)
  poll_loop(market_data_socket, order_socket)
  
  
  
    