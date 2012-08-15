import time 
import collections

import sys 
from strategy import Strategy 
from order_manager import BID, OFFER 
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
  
  def __str__(self):
    return "Cross(bid = %s, offer = %s)" % (self.bid, self.offer)    
    
symbols_to_bids = {} 
# map each symbol (ie USD/JPY) to a dictionary from venue_id's to offer entries
symbols_to_offers = {}
# set of symbols whose data has been updated since the last time the function 
# 'find_best_crossed_pair' ran 
updated_symbols = set([])
current_cross = None
    
def md_update(bbo):
  sys.stdout.write('.')
  sys.stdout.flush()

  timestamp = time.time()

  symbol, venue_id = bbo.symbol, bbo.bid_venue_id  
  new_bid = Entry(bbo.bid_price, bbo.bid_size, venue_id, bbo.symbol, timestamp)  
  new_offer = Entry(bbo.ask_price, bbo.ask_size, venue_id, bbo.symbol, timestamp)
  
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



from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
parser.add_argument('--max-order-size', type=int, default=10000000, dest='max_order_size')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")
parser.add_argument('--min-cross-magnitude', type=float, default = 50, dest = 'min_cross_magnitude')
parser.add_argument('--max-order-lifetime', type=float, default = 5.0, dest='max_order_lifetime')


import atexit  
if __name__ == '__main__':
  args = parser.parse_args()
  uncrosser = Strategy(STRATEGY_ID)
  uncrosser.connect(args.config_server)
  atexit.register(uncrosser.close_all)
  uncrosser.synchronize_market_data(md_update, args.startup_wait_time)
  def place_orders(order_manager):
    outgoing_logic(order_manager, args.min_cross_magnitude, args.order_delay, args.max_order_lifetime)
  uncrosser.main_loop(md_update, place_orders)
  
