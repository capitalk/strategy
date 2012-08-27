import time 
import logging
import sys 
from market_data import MarketData 
from strategy_loop import Strategy 
from proto_objs.capk_globals_pb2 import BID, ASK
from logging_helpers import create_logger

logger = create_logger("uncross", logging.INFO, "uncross.log", logging.INFO)



STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022'
md = MarketData()
# we get an order_manager back from Strategy.connect
order_manager = None
updated_symbols = set([])
cross = None

# a pair of entries for bid and offer
class Cross:
  def __init__(self, bid_entry, offer_entry):
    self.bid_entry = bid_entry
    self.offer_entry = offer_entry
    self.start_time = time.time()
    # these get filled in after the orders get created/sent 
    self.send_time = None
    self.bid_order_id = None
    self.offer_order_id = None
    self.sent = False
    
    # this ID only gets set if we're trying to kill the cross
    # after being unequally filled and thus need a new order
    # to get out of the position 

    self.rescue_order_id = None
    self.rescue_start_time = None 

  def set_rescue_order_id(self, rid): 
    self.rescue_order_id = rid
    self.rescue_start_time = time.time()

  def send(self):
    assert not self.sent, "Can't sent the same cross twice"
    # send the damn thing 
    bid_entry = self.bid_entry
    offer_entry = self.offer_entry
    qty = min(bid_entry.size, offer_entry.size)
    low = offer_entry.price
    high = bid_entry.price
    assert low < high, \
      "Isn't this supposed to be an uncrosser? offer %s not less than bid %s " % \
      (low, high)
    symbol = bid_entry.symbol 
    assert symbol == offer_entry.symbol, \
      "Can't uncross two different symbols! (%s, %s)" % \
      (bid_entry.symbol, offer_entry.symbol)
    # NB: Send a BID order to transact with the offer in the order book
    # and vice versa (send an ASK to transact with the available bid)
    bid_id = order_manager.send_new_order(bid_entry.venue, symbol, ASK, low, qty)
    self.bid_order_id = bid_id
    offer_id = order_manager.send_new_order(offer_entry.venue, symbol, BID, high, qty)
    self.offer_order_id = offer_id 
    
    self.send_time = time.time()
    self.sent = True
  
  def send_when_ready(self, wait_time):
    ready_to_send = self.start_time + wait_time >= time.time()
    if ready_to_send: 
      self.send()
      logger.info("Sent orders for %s",  self)
    else:
      logger.info("Waiting to send orders for %s", self)
    
  
  def __str__(self):
    return "Cross(bid = %s, offer = %s)" % (self.bid_entry, self.offer_entry)    
    

    
def md_update_wrapper(bbo):
  """Update market data and add any changed symbols to 'updated_symbols' set"""
  changed = md.update(bbo)
  if changed:
    updated_symbols.add(bbo.symbol)

def find_best_crossed_pair(min_cross_magnitude, max_size = 10 ** 8):
  assert cross is None
  if len(updated_symbols) == 0: return
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
          logger.info("Cross found: %s %d @ %s", symbol, cross_size, cross_magnitude)
          if yen_pair: cross_magnitude /= 80
          if cross_magnitude > best_cross_magnitude:
            best_cross = Cross(bid_entry = bid_entry, offer_entry = offer_entry)
            best_cross_magnitude = cross_magnitude 
  if best_cross is not None:
    logger.info("Best cross: %s", best_cross)
    if best_cross_magnitude < min_cross_magnitude: 
      best_cross = None
  updated_symbols.clear()
  return best_cross


def close_unbalanced_cross(bigger, smaller):
  order_manager.cancel_if_alive(bigger.id)
  if order_manager.is_alive(smaller.id):
    rescue_id = order_manager.liquidate_order(md, smaller.id, bigger.filled_qty)
    cross.set_rescue_order_id(rescue_id)
  else:
    side, symbol, venue = smaller.side, smaller.symbol, smaller.venue
    # none of the smaller side's IDs are alive, so we need to put in a new order
    logger.info(\
      "Dead order for %s on %s (side = %s, %d) needs %s",
       symbol, venue, side, smaller.filled_qty, bigger.filled_qty)
    price = md.liquidation_price(side, symbol, venue)
    qty_diff = bigger.filled_qty - smaller.filled_qty 
    assert qty_diff > 0, \
      "Why did you call close_unbalanced_cross if there's no fill difference?"
    rescue_id = \
      order_manager.send_new_order(venue, symbol, side, price, qty_diff)
    cross.set_rescue_order_id(rescue_id)

  logger.info("Rescue order: %s", order_manager.get_order(cross.rescue_order_id))  
  
def kill_cross():
  """
  If both orders have been filled to the same qty then killing the cross
  is trivial, just cancel the orders if they've been partially filled. 
  HOWEVER, trouble starts when they get filled with unequal quantities--
  to avoid holding a position we need cancel the larger order and replace
  the smaller at a price which is likely to be filled.
  NB: Optionally returns the ID of a hedge order which gets placed if 
  the sides have different fill amounts. 
  """
  assert cross.sent, "Can't kill a cross before you send it"
  bid = order_manager.get_order(cross.bid_order_id) 
  ask = order_manager.get_order(cross.offer_order_id)
  bid_qty = bid.filled_qty
  ask_qty = ask.filled_qty
  
  if bid.filled_qty == ask.filled_qty:
    order_manager.cancel_if_alive(bid.id)
    order_manager.cancel_if_alive(ask.id)
  elif bid_qty > ask_qty:
    close_unbalanced_cross(bid, ask)
  else:
    close_unbalanced_cross(ask, bid)

def both_dead(bid, offer):
  """Takes two dead orders and optionally places a hedge order if they
     are unevenly filled
  """
  global cross 
  logger.info ("Both sides dead")
  if bid.filled_qty > offer.filled_qty:
    close_unbalanced_cross(bid, offer)   
  elif bid.filled_qty < offer.filled_qty:
    close_unbalanced_cross(offer, bid)
  else:
    price_delta = offer.price - bid.price 
    profit = bid.filled_qty * price_delta 
    logger.info("Cross completed! Profit = %s %s",
     profit,  bid.symbol)
    cross = None
      
def manage_active_cross(max_order_lifetime):
  global cross
  if cross.rescue_order_id:
    
    # one order got rejected or some other weird situation which 
    # required us to hedge against a lopsided position 
    order = order_manager.get_order(cross.rescue_order_id)
    rescue_pending = cross.rescue_order_id in order.pending_ids 
    rescue_alive = order_manager.is_alive(cross.rescue_order_id)
    rescue_dead = not (rescue_pending or rescue_alive)
    rescue_expired = time.time() - order.rescue_start_time >= 5
    # if the order is filled, or the rescue has expired, 
    # or rescue order has died, give up on it!
    if order.filled_qty == order.qty:
      logging.info("Rescue succeeded: %s" % cross.rescue_order_id)
      cross = None
    elif rescue_dead or rescue_expired: 
      logging.info("Rescue failed: %s" % cross.rescue_order_id)
      cross.rescue_order_id = None
    else:
      sys.stdout.write('r')
      sys.stdout.flush()
  elif time.time() >= cross.send_time + max_order_lifetime: 
    # expired!
    kill_cross()
  else:
    
    bid_id = cross.bid_order_id
    bid_alive = order_manager.is_alive(bid_id)
    bid = order_manager.get_order(bid_id)
    
    offer_id = cross.offer_order_id 
    offer_alive = order_manager.is_alive(offer_id)
    offer = order_manager.get_order(offer_id)
    if not (bid_alive or offer_alive ): 
      both_dead(bid, offer)
    elif bid_alive and offer_alive:
      #logger.info("Both orders alive for %s, bid filled = %d, ask filled = %d",
      #  bid.symbol, bid.filled_qty, offer.filled_qty)  
      sys.stdout.write("-")
      sys.stdout.flush()
    elif bid_alive and offer.filled_qty < offer.qty:
      # offer is dead with a partial fill 
      logger.info("Bid alive, offer dead with %d/%d filled", 
        offer.filled_qty, offer.qty)
      kill_cross()
    elif offer_alive and bid.filled_qty < bid.qty:
      # bid is dead with a partial fill 
      logger.info("Offer alive, bid dead with %d/%d filled", 
        offer.filled_qty, offer.qty)
      kill_cross()
    else:
      # only one side alive, waiting for a fill on the other side 
      sys.stdout.write('!')
      sys.stdout.flush()
      #logger.info("Still waiting for a fill (bid %d/%d, offer %d/%d)", 
      #  bid.filled_qty, bid.qty, offer.filled_qty, offer.qty)
    

def outgoing_logic(min_cross_magnitude, 
      new_order_delay = 0, 
      max_order_lifetime = 5, 
      max_order_qty = 10 ** 8):
  global cross
  if cross is not None:
    if not cross.sent:
      cross.send_when_ready(new_order_delay)
    else:
      manage_active_cross(max_order_lifetime)
  # this has to come second since the functions above might find the 
  # cross is finished and reset the global 'cross' variable to None    
  if cross is None:
    cross = find_best_crossed_pair(min_cross_magnitude, max_order_qty)
    # if there's no delay, send orders immediately, 
    # otherwise it will happen on the next update cycle 
    if new_order_delay == 0 and cross is not None: cross.send()

from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
parser.add_argument('--max-order-qty', type=int, default=10 ** 8, dest='max_order_qty')
parser.add_argument('--order-delay', type=float, default=0.0, dest='order_delay', 
  help='How many milliseconds should I delay orders by?')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")
parser.add_argument('--min-cross-magnitude', type=float, default = 35, dest = 'min_cross_magnitude')
parser.add_argument('--max-order-lifetime', type=float, default = 5.0, dest='max_order_lifetime')


import atexit  
if __name__ == '__main__':
  args = parser.parse_args()
  strategy = Strategy(STRATEGY_ID)
  order_manager = strategy.connect(args.config_server)
  # TODO: Figure out why zmq sockets hang on exit
  # atexit.register(strategy.close_all)
  def place_orders():
    outgoing_logic(args.min_cross_magnitude, 
      args.order_delay, 
      args.max_order_lifetime, 
      args.max_order_qty)
  strategy.run(md_update_wrapper, place_orders)
  
  
