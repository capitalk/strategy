import time 

import sys 
from market_data import MarketData 
from strategy import Strategy 
from proto_objs.capk_globals_pb2 import BID, ASK


# a pair of entries for bid and offer
class Cross:
  def __init__(self, bid, offer):
    self.bid = bid
    self.offer = offer
    self.start_time = time.time()
    self.send_time = None
   
  def status_sent(self):
    self.send_time = time.time()
  
  def __str__(self):
    return "Cross(bid = %s, offer = %s)" % (self.bid, self.offer)    
    


STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022'
md = MarketData()
updated_symbols = set([])
current_cross = None

def md_update(bbo):
  """Update market data and add any changed symbols to 'updated_symbols' set"""
  sys.stdout.write('.')
  sys.stdout.flush()
  changed = md.update(bbo)
  if changed:
    updated_symbols.add(bbo.symbol)

def find_best_crossed_pair(min_cross_magnitude, max_size = 100000000):
  assert current_cross is None
  if len(updated_symbols) == 0: return
  # print "UPDATED SYMBOLS", updated_symbols
  best_cross = None
  best_cross_magnitude = 0
  for symbol in updated_symbols:
     
    yen_pair = "JPY" in symbol
    sorted_bids = md.sorted_bids(symbol)
    sorted_offers = md.sorted_offers(symbol)
    for bid_entry in sorted_bids:
      for offer_entry in sorted_offers:
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
    bid = current_cross.bid
    offer = current_cross.offer
    
    if current_cross.send_time is None and (current_cross.start_time + new_order_delay >= curr_time):
      # send the damn thing 
      print "Sending orders for %s" % current_cross
      om.send_new_order(bid.venue, bid.symbol, BID, bid.price, bid.size)
      om.send_new_order(offer.venue, offer.symbol, ASK, offer.price, offer.size)
      current_cross.status_sent()
      
    # if we've already sent an order, check if it's expired
    elif current_cross.send_time is not None:
      expired = curr_time >= current_cross.send_time + max_order_lifetime 
      n_open = len(om.live_order_ids)
      sys.stdout.write(str(n_open))
      sys.stdout.flush()
      if n_open == 0:
        current_cross = None
      elif n_open == 1 and expired:
        om.liquidate_immediately(md) 
      elif n_open == 2 and expired:
        om.cancel_everything()
      elif n_open > 2:
        print "WARNING: Didn't expect to have %d open orders simultaneously" % n_open


from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
parser.add_argument('--max-order-size', type=int, default=10000000, dest='max_order_size')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")
parser.add_argument('--min-cross-magnitude', type=float, default = 35, dest = 'min_cross_magnitude')
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
  
