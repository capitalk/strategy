import sys 
import logging
import collections 
import time 
from proto_objs.capk_globals_pb2 import BID, ASK

Entry = collections.namedtuple('Entry', ('price', 'size', 'venue', 'symbol', 'timestamp'))

class MarketData:
  """
  Aggregated best-bid, best-offer information from multiple venues
  """
  def __init__(self):
    self.symbols_to_bids = {} 
    # map each symbol (ie USD/JPY) to a dictionary from venue's to offer entries
    self.symbols_to_offers = {}
    # set of symbols whose data has been updated since the last time the function 
    # 'find_best_crossed_pair' ran 
    
  def update(self, bbo, print_dot = True):
    timestamp = time.time()

<<<<<<< HEAD
    symbol, venue = bbo.symbol, bbo.bid_venue_id
=======
    symbol, venue = bbo.symbol, bbo.bid_venue_id 
>>>>>>> 7a1f6f274ee1ca0e121788746cf4089fba58b2cd
    if venue == 0:
      logging.warning("Venue ID was 0, changing to 890778")
      venue = 890778
    bid_size, bid_price = bbo.bid_size, bbo.bid_price
    ask_size, ask_price = bbo.ask_size, bbo.ask_price
    new_bid = Entry(bid_price, bid_size, venue, bbo.symbol, timestamp)  
    new_offer = Entry(ask_price, ask_size, venue, bbo.symbol, timestamp)
    if print_dot:
      sys.stdout.write('.')
      sys.stdout.flush()    
    bids = self.symbols_to_bids.setdefault(symbol, {})
    old_bid = bids.get(venue)
    changed = False 
    if old_bid != new_bid:
      changed = True
      bids[venue] = new_bid
    
    offers = self.symbols_to_offers.setdefault(symbol, {})
    old_offer = offers.get(venue)
    if old_offer != new_offer:
      changed = True
      offers[venue] = new_offer
    return changed
 
  def get_bid(self, symbol, venue = None):
    venues_to_bids = self.symbols_to_bids[symbol]
    if venue: return venues_to_bids[venue]
    else: return max(venues_to_bids.values())
  
  def get_offer(self, symbol, venue = None):
    venues_to_offers = self.symbols_to_offers[symbol]
    if venue: return venues_to_offers[venue]
    else: return min(venues_to_offers.values())
    
  def sorted_bids(self, symbol):
    bid_venues = self.symbols_to_bids.get(symbol, [])
    return sorted(bid_venues.values(), key=lambda e: e.price, reverse=True)
  
  def sorted_offers(self, symbol):
    offer_venues = self.symbols_to_offers.get(symbol, [])
    return sorted(offer_venues.values(), key=lambda e: e.price)
  
  def collect_best_bids(self):
    result = {}
    for sym in self.symbols_to_bids.keys():
      result[sym] = self.sorted_bids(sym)[0]
    return result 
    
  def collect_best_offers(self):  
    result = {}
    for sym in self.symbols_to_offers.keys():
      result[sym] = self.sorted_offers(sym)[0]
    return result 
    
  def bid_liquidation_price(self, symbol, venue = None):
    best_offer = self.get_offer(symbol, venue) 
    # submit a price 3 percent-pips worse than the best to improve our 
    # chances of a fill
    return best_offer.price * 1.0003 
    
    
  def offer_liquidation_price(self, symbol, venue = None):
    best_bid = self.get_bid(symbol, venue) 
    return best_bid.price * 0.9997 
    
  def liquidation_price(self, side, symbol, venue):
    if side == ASK: return self.offer_liquidation_price(symbol, venue)
    else: return self.bid_liquidation_price(symbol, venue)
    
