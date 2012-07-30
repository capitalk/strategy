from proto_objs import spot_fx_md_1_pb2
import gevent 
from gevent_zeromq import zmq 
import time 
import collections

#def read_symbols(filename):
#  f = open(filename)
#  syms = [] 
#  for line in f.readlines():
#    line = line.strip()
#    if len(line) == 0: continue
#    elif len(line) != 7 or line[3] != "/": 
#      raise RuntimeError("Invalid ccy format " + line)
#    else:
#      syms.append( line)
#  f.close()
#  print syms 
#  return syms

context = zmq.Context()

def make_subscriber(addr = "tcp://127.0.0.1", port = 5273, symbols = None):
  socket = context.socket(zmq.SUB)
  target = "%s:%s" % (addr, port)
  print "Making socket for", target
  socket.connect(target)
  if symbols is None:
    print "Subscribing to all messages" 
    socket.setsockopt(zmq.SUBSCRIBE, "")
  else:
    # NOTE: This is invalid if we're not subscribing to the aggregated book
    for sym in symbols:
      print "Subscribing to", sym 
      socket.setsockopt(zmq.SUBSCRIBE, sym)
  return socket 


symbols_to_venues = {} 
updated_symbols = set([])
Entry = collections.namedtuple('Entry', ('bid', 'offer', 'bid_size', 'offer_size'))

def receive_market_data(md_addr, md_port):
  md_socket = make_subscriber(md_addr, md_port) 
  while True:
    msg_parts = md_socket.recv_multipart()
    if len(msg_parts) != 1:
      err_msg = "Received unrecognized message of length" + str(len(msg_parts))+ " : " + str(msg_parts)
      raise RuntimeError(err_msg)

    bbo = spot_fx_md_1_pb2.instrument_bbo();
    bbo.ParseFromString(msg_parts[0]);
    
    print len(msg_parts), msg_parts
    print bbo
    print 
   
    print "symbol", bbo.symbol
    print "venue", bbo.bb_venue_id
    symbol, venue_id = bbo.symbol, bbo.bb_venue_id  
    assert symbol
    assert venue_id 
    venues = symbols_to_venues.get(symbol, {})
    entry = Entry(bbo.bb_price, bbo.ba_price, bbo.bb_size, bbo.ba_size)
    if venue_id not in venues:
      venues[venue_id] = entry
      updated_symbols.add(symbol)
    else:
      old_entry = venues[venue_id]
      if old_entry != entry:
        venues[venue_id] = entry 
        updated_symbols.add(symbol)
    gevent.sleep(0)

def look_for_crossed_markets(order_addr, order_port):
  if order_addr and order_port:
    order_socket = make_subscriber(order_addr, order_port)
  else:
    order_socket = None
  while True:
    if len(updated_symbols) > 0:
      print "UPDATED SYMBOLS", updated_symbols
      print symbols_to_venues
      updated_symbols.clear()
      for (symbol, venues) in symbols_to_venues.iteritems():
        best_bid = None; best_offer = None
        best_bid_size = None; best_offer_size = None
        best_bid_venue = None; best_offer_venue = None
        for (i, (venue_id, entry)) in enumerate(venues.iteritems()):
          bid, offer, bid_size, offer_size = entry 
          if i == 0:
            best_bid = bid; best_offer = offer
            best_bid_size = bid_size; best_offer_size = offer_size
            best_bid_venue = venue_id; best_offer_venue = venue_id
          if bid > best_bid:
            best_bid = bid; best_bid_venue = venue_id
          elif bid == best_bid and bid_size > best_bid_size:
            best_bid_size = bid_size; best_bid_venue = venue_id

          if offer < best_offer:
            best_offer = offer; best_offer_venue = venue_id 
          elif offer == best_offer and offer_size > best_offer_size:
            best_offer_size = offer_size; best_offer_venue = venue_id
        if best_bid > best_offer:
          print symbol, "crossed with bid=", best_bid, "(venue =", best_bid_venue,") and offer =", best_offer, "(venue =", best_offer_venue, ")"
    gevent.sleep(0)

def receive_order_updates(order_addr, order_port):
  socket = make_subscriber(order_addr, order_port)
  while True:
    msg_parts = socket.recv_multipart()
    print msg_parts
    gevent.sleep(0)

HELLO_ACK = 0xF1

def say_hello(venue_id):
  STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022' 
  HELLO = 0xF0
  socket.send([venue_id, HELLO, STRATEGY_ID])


from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--market-data-addr', type=str, default='tcp://127.0.0.1', dest = 'md_addr')
parser.add_argument('--market-data-port', type=int,  required=True, dest='md_port')
parser.add_argument('--order-engine-addr', type=str, default='tcp://127.0.0.1', dest='order_addr')
parser.add_argument('--order-engine-port', type=int, default=None, dest='order_port')
parser.add_argument('-s', '--symbols-file', type=str, default=None, dest='symbols_file')

if __name__ == '__main__':
  args = parser.parse_args()
  # symbols = read_symbols(args.symbols_file) if args.symbols_file else None
  threads = [
    gevent.spawn(receive_market_data, args.md_addr, args.md_port), 
    gevent.spawn(look_for_crossed_markets, args.order_addr, args.order_port)
  ]
  if args.order_port:
    order_thread = gevent.spawn(receive_order_updates, args.order_addr, args.order_port)
    threads.append(order_thread)
  else:
    print "No port given for Order Engine, just listening to market data"
  gevent.joinall(threads)


