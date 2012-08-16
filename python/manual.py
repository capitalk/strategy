
from os import system
import curses
from market_data import MarketData, Entry
from strategy import Strategy 
import sys 
import atexit 


STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e023'  
strategy = Strategy(STRATEGY_ID)
md = MarketData()

screen = None
order_window = None
action_window = None
md_window = None


def init_ui():
  global screen 
  global order_window
  global action_window 
  global md_window
  screen = curses.initscr()
  
  action_window = curses.newwin(20, 70, 2, 2)
  md_window = curses.newwin(34, 100, 2, 72)
  order_window = curses.newwin(11, 170, 35, 2)
  action_window.timeout(250)

def print_market_data():
  best_bids = md.collect_best_bids()
  best_offers = md.collect_best_offers() 
  
  md_window.erase()
  md_window.border(0)
  syms = set(best_bids.keys()).union(set(best_offers.keys()))
  md_window.addstr(2, 3, "Order Book")

  for (i, sym) in enumerate(sorted(syms)):
    bid_entry = best_bids.get(sym)
    offer_entry = best_offers.get(sym)
    #raise RuntimeError(str( bid_entry) + "/" + str( offer_entry))
      
    if bid_entry:
      bid_str = "%d @ %s (%s)" % (bid_entry.size, bid_entry.price, bid_entry.venue)
    else:
      bid_str = "<none>"
    if offer_entry:
      offer_str = "%d @ %s (%s)" % (offer_entry.size, offer_entry.price, offer_entry.venue)
    else:
      offer_str = "<none>"
    md_window.addstr(4+i, 5, "%s : bid = %s, offer = %s" % (sym, bid_str, offer_str))
  md_window.refresh()
 
def print_action_menu():
  action_window.erase()
  action_window.border(0)
  action_window.timeout(250)
  action_window.addstr(2,3,"Actions")
  
  action_window.addstr(4,5,"N - New order")
  action_window.addstr(6,5,"C - Cancel single order")
  action_window.addstr(8,5, "A - Cancel all open orders")
  action_window.addstr(10,5,"Q - Quit")
  action_window.refresh()
 


# keep our own dict of live_orders mapping their displayed numbers to
# internal UUIDs 
displayed_live_orders = {} 
def print_live_orders(order_manager):
  displayed_live_orders.clear()
  order_window.erase()
  order_window.border(0)
  order_window.addstr(2, 3, "Live Orders")
  print len(order_manager.live_order_ids)
  for (i, order_id) in enumerate(order_manager.live_order_ids):
    order = order_manager.get_order(order_id)
    displayed_id = i+1
    msg = "%d) venue = %d, symbol = %s, side = %s, price = %s, size = %s" % \
      (displayed_id, order.venue_id, order.symbol, order.side, order.price, order.qty)
    displayed_live_orders[displayed_id] = order_id
    order_window.addstr(4 + i, 5, msg)  
  order_window.refresh()


def dialog(fn, *args, **kwds):
  action_window.erase()
  action_window.border(0)
  action_window.timeout(-1)
  fn(*args, **kwds)
  action_window.timeout(250)
  
@dialog
def new_order_dialog(order_manager):
  action_window.addstr(2,3, "New")
  bids = md.collect_best_bids()
  bid_symbols = bids.keys()
      
  assert len(bid_symbols) > 0
  default_symbol = bid_symbols[0]
  default_venue = bids[default_symbol].venue
  default_price = bids[default_symbol].price
  default_size = bids[default_symbol].size 

  action_window.addstr(4,5, "Venue [%s]:" % default_venue)
  venue_str = action_window.getstr(4, 25).strip()
  if len(venue_str) == 0:
    venue = default_venue
  else:
    venue = int(venue_str)
  action_window.addstr(6,5, "Symbol [%s]:" % default_symbol)
  symbol = action_window.getstr(6, 25).strip()
  if len(symbol) == 0: symbol = default_symbol 
  action_window.addstr(8,5, "Side [bid]:")
  side_str = action_window.getstr(8, 25).strip()
  if len(side_str) == 0:
    side = 0
  else:
    side = side_str in ['1', 'a', 'ask', 'A', 'o', 'offer', 'O']
      
  action_window.addstr(10,5, "Price [%s]:"% default_price)
  price_str = action_window.getstr(10, 25).strip()
  if len(price_str) == 0:
    price = default_price
  else:
    price = float(price_str)
      
  action_window.addstr(12,5, "Size [%s]:" % default_size)
  size_str = action_window.getstr(12,25).strip()
  if len(size_str) == 0:
    size = default_size
  else:
    size = int(size_str)
  order_manager.send_new_order(venue, symbol, side, price, size)

@dialog
def cancel_dialog(order_manager):
  action_window.addstr(2,3, "Cancel")
  action_window.addstr(4,3, "Order num:")
  num_str = action_window.getstr(4, 13).strip()
  num = int(num_str)
  assert num in displayed_live_orders
  order_id = displayed_live_orders[num]
  order_manager.send_cancel(order_id)


def ui_update(order_manager):
  try:
    print_market_data()
    print_live_orders(order_manager)
    print_action_menu()

    x = action_window.getch()
    if x <= 0:
      return 
    x = chr(x).upper()
    if x == 'A':
      order_manager.cancel_everything()
    elif x == 'N':
      new_order_dialog(order_manager)
    elif x == 'C':
      cancel_dialog(order_manager)
    elif x =='Q':
      curses.endwin()
      exit(0)
      
  except:  
    curses.endwin()
    raise 

from argparse import ArgumentParser 
parser = ArgumentParser(description='Manual order entry') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
if __name__ == '__main__':
  args = parser.parse_args()
  strategy.connect(args.config_server)
  
  #atexit.register(strategy.close_all)
  strategy.synchronize_market_data(md.update)
  init_ui()
  strategy.main_loop(md.update, ui_update)

