import collections 
import time 

Entry = collections.namedtuple('Entry', ('price', 'size', 'venue', 'symbol', 'timestamp'))

class MarketData:
  """
  Aggregated best-bid, best-offer information from multiple venues
  """
  def __init__(self):
    self.symbols_to_bids = {} 
    # map each symbol (ie USD/JPY) to a dictionary from venue_id's to offer entries
    self.symbols_to_offers = {}
    # set of symbols whose data has been updated since the last time the function 
    # 'find_best_crossed_pair' ran 
    
  def update(self, bbo, print_dot = False):
    timestamp = time.time()

    symbol, venue_id = bbo.symbol, bbo.bid_venue_id  
    new_bid = Entry(bbo.bid_price, bbo.bid_size, venue_id, bbo.symbol, timestamp)  
    new_offer = Entry(bbo.ask_price, bbo.ask_size, venue_id, bbo.symbol, timestamp)
  
    bids = self.symbols_to_bids.setdefault(symbol, {})
    old_bid = bids.get(venue_id)
    changed = False 
    if old_bid != new_bid:
      changed = True
      bids[venue_id] = new_bid
    
    offers = self.symbols_to_offers.setdefault(symbol, {})
    old_offer = offers.get(venue_id)
    if old_offer != new_offer:
      changed = True
      offers[venue_id] = new_offer
    return changed
  
  def sorted_bids(self, symbol):
    bid_venues = self.symbols_to_bids.get(symbol, [])
    return sorted(bid_venues.items(), key=lambda (v,e): e.price, reverse=True)
  
  def sorted_offers(self, symbol):
    offer_venues = self.symbols_to_offer.get(symbol, [])
    return sorted(offer_venues.items(), key=lambda (v,e): e.price)
  
  def collect_best_best_bids(self):
    result = {}
    for sym in self.symbols_to_bids.keys():
      result[sym] = self.sorted_bids(sym)[0]
    return result 
    
  def collect_best_offers(self):  
    result = {}
    for sym in self.symbols_to_offers.keys():
      result[sym] = self.sorted_offers(sym)[0]
    return result 
    