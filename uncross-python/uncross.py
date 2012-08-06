from proto_objs import spot_fx_md_1_pb2
import zmq 
import time 
import collections
import order_manager
import order_engine_constants


STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022'

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
  print "Venue", bbo.bid_venue_id
  symbol, venue_id = bbo.symbol, bbo.bid_venue_id  
  new_bid = Entry(bbo.bid_price, bbo.bid_size, venue_id, bbo.symbol, timestamp)  
  new_offer = Entry(bbo.ask_price, bbo.ask_size, venue_id, bbo.symbol, timestamp)
  
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
    sorted_bids = sorted(bid_venues.items(), key=lambda (v,e): e.price, reverse=True)
    # offers from lowest to highest
    sorted_offers = sorted(offer_venues.items(), key=lambda (v,e): e.price)
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

def outgoing_logic(order_sockets, order_manager, new_order_delay = 0,  max_order_lifetime = 5):
  curr_time = time.time()
  global current_cross 
  if current_cross is None:
    current_cross = find_best_crossed_pair()

  # these if statements look repetitve but remember that find_best_crossed_pair
  # might return None if there is no good cross
  if current_cross is not None:
    bid = current_cross.bid
    offer = current_cross.offer
    
    if current_cross.send_time is None and (current_cross.start_time + new_order_delay >= curr_time):
      # send the damn thing 
      print "Sending orders for %s" % current_cross
      order_manager.send_new_order(bid.venue, bid.symbol, order_manager.BID, bid.price, bid.size)
      order_manager.send_new_order(offer.venue, offer.symbol, order_manager.OFFER, offer.price, offer.size)
      current_cross.status_sent()
      
    # if we've already sent an order, check if it's expired
    elif current_cross.send_time is not None:
      expired = curr_time >= current_cross.send_time + max_order_lifetime 
      n_open = len(order_manager.live_order_ids)
      if n_open == 0:
        current_cross = None
      elif n_open == 1 and expired:
        order_manager.liquidate_immediately(order_manager.live_order_ids.pop())
      elif n_open == 2 and expired:
        order_manager.cancel_everything()
      else:
        raise RuntimeError("Didn't expect to have %d open orders simultaneously" % n_open)
        
def main_loop(market_data_socket, order_sockets, new_order_delay = 0, max_order_lifetime = 5):
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  for order_socket in order_sockets:
    poller.register(order_socket, zmq.POLLIN)
  om = order_manager.OrderManager(STRATEGY_ID, order_sockets)
  
  while True:
    ready_sockets = poller.poll(1000)
    for (socket, state) in ready_sockets:
      assert state == zmq.POLLIN
      if socket == market_data_socket:
        print "POLLIN: market data"
        msg = market_data_socket.recv()
        bbo = spot_fx_md_1_pb2.instrument_bbo();
        bbo.ParseFromString(msg)
        update_market_data(bbo)
      else:
        print "POLLIN: order engine"
        [tag, msg] = order_socket.recv_multipart()
        order_manager.received_message_from_order_engine(tag, msg)
    outgoing_logic(om, new_order_delay, max_order_lifetime)


def say_hello(socket):
  socket.send_multipart([order_engine_constants.STRATEGY_HELO, STRATEGY_ID])
  ready_flags = socket.poll(3000)
  if zmq.POLLIN not in ready_flags:
    raise RuntimeError("Didn't get response to HELO from " + str(socket))
  [tag, venue_id] = socket.recv_multipart()
  assert tag == order_engine_constants.STRATEGY_HELO_ACK
  return venue_id

def connect_to_order_engine(addr):
  order_socket = context.socket(zmq.DEALER)
  print "Connecting order engine socket to", addr
  order_socket.connect(addr)
  venue_id = say_hello(order_socket)
  if not venue_id:
    raise RuntimeError("Couldn't say HELO to order engine at " + addr)
  print "...got venue_id =", venue_id
  return order_socket, venue_id 

def ping(socket, name = None):
  t0 = time.time()
  socket.send(order_engine_constants.PING)
  ready_flags = socket.poll(3000)  
  if zmq.POLLIN not in ready_flags:
    if not name: name = "<not given>"
    raise RuntimeError("Timed out waiting for ping ack from %s" % name)
  else:
    assert socket.recv() == order_engine_constants.PING_ACK
  return time.time() - t0 
  
def connect_to_order_engine_controller(addr):
  order_control_socket = context.socket(zmq.REQ)
  print "Connecting control socket to %s" %  addr 
  order_control_socket.connect(addr)
  ping(order_control_socket)
  return order_control_socket 
    
def init(md_addrs, order_engine_addrs, symbols = None):
  md_socket = context.socket(zmq.SUB)
  for addr in args.md_addrs:
    print "Connecting to market data socket %s" % addr
    md_socket.connect(addr)
  
  if symbols is None:
    print "Subscribing to all messages" 
    md_socket.setsockopt(zmq.SUBSCRIBE, "")
  else:
    raise RuntimeError("Removed support for selective subscriptions!")

  order_sockets = {}
  order_control_sockets = {}
  for addr in args.order_engine_addrs:
    try: 
      parts = addr.split(":")
      base_addr = "".join(parts[:-1])
      # assume the REP socket of an order engine connects at a port 2000 less 
      # than the DEALER socket
      port = int(parts[-1]) - 2000
      control_addr = "%s:%s"  % (base_addr, port)
    except:
      raise RuntimeError("Malformed order engine address " + addr)
    
    order_socket, venue_id = connect_to_order_engine(addr)
    order_control_socket = connect_to_order_engine_controller(control_addr)
    order_sockets[venue_id] = order_socket
    order_control_sockets[venue_id] = order_control_socket  
  return md_socket, order_sockets, order_control_sockets
  
    

from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--market-data', type=str, nargs='*', default=[],  dest = 'md_addrs')
parser.add_argument('--order-engine', type=str, nargs='*', default = [], dest='order_engine_addrs')
parser.add_argument('--max-order-size', type=int, default=10000000, dest='max_order_size')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")

  
if __name__ == '__main__':
  args = parser.parse_args()
  assert len(args.md_addrs) > 0
  md_socket, order_sockets, _ = init(args.md_addrs, args.order_engine_addrs)
  synchronize_market_data(md_socket, args.startup_wait_time)
  main_loop(md_socket, order_sockets)
  
  
  
    
