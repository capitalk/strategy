
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
  order_window = curses.newwin(40, 70, 2, 122)
  
  action_window = curses.newwin(40, 50, 2, 2)
  md_window = curses.newwin(40, 70, 2, 52)
  
  for w in [order_window, action_window, md_window]:
    w.timeout(500)

def print_market_data():
  best_bids = md.collect_best_bids()
  best_offers = md.collect_best_offers() 
  
  md_window.erase()
  md_window.border(0)
  syms = set(best_bids.keys()).union(set(best_offers.keys()))
  md_window.addstr(2, 3, "Order Book")

  for (i, sym) in enumerate(syms):
    bid_entry = best_bids.get(sym)
    offer_entry = best_offers.get(sym)
    if bid_entry:
      bid_str = "%d @ %s (%s)" % (bid_entry.size, bid_entry.price, bid_entry.venue)
    else:
      bid_str = "<none>"
    if offer_entry:
      offer_str = "%d @ %s (%s)" % (offer_entry.size, offer_entry.price, offer_entry.venue)
    else:
      offer_str = "<none>"
    md_window.addstr(4+i*2, 5, "%s : bid = %s, offer = %s" % (sym, bid_str, offer_str))
  md_window.refresh()
 
def print_action_menu():
  action_window.erase()
  action_window.border(0)
  action_window.addstr(2,3,"Actions")
  
  action_window.addstr(4,5,"N - New order")
  action_window.addstr(6,5,"C - Cancel order")
  action_window.addstr(8,5,"Q - Quit")
  action_window.refresh()
 
def print_live_orders(order_manager):
  
  order_window.erase()
  order_window.border(0)
  order_window.addstr(2, 3, "Live Orders")
  
  for (i, order_id) in enumerate(order_manager.live_order_ids):
    order = order_manager.get_order(order_id)
    msg = "id = %s, venue = %d, symbol = %s, side = %s, price = %s, size = %s" % \
      (order.id, order.venue_id, order.symbol, order.side, order.price, order.qty)
    order_window.addstr(4 + i*2, 5, msg)  
  

def ui_update(order_manager):
  try:
    print_market_data()
    print_live_orders(order_manager)
    print_action_menu()

    x = action_window.getch()

    if x in [ord('N'), ord('n')]:
      action_window.erase()
      action_window.border(0)
      action_window.addstr(2,3, "New")
      action_window.addstr(4,5, "Venue:")
      venue_str = action_window.getstr(4, 15).strip()
      venue = int(venue_str)
      action_window.addstr(6,5, "Symbol:")
      symbol = action_window.getstr(6, 15).strip()
      
      action_window.addstr(8,5, "Side:")
      side_str = action_window.getstr(8, 15).strip()
      side = side_str in ['1', 'a', 'ask', 'A', 'o', 'offer', 'O']
      
      action_window.addstr(10,5, "Price:")
      price_str = action_window.getstr(10, 15).strip()
      price = float(price_str)
      
      action_window.addstr(12,5, "Size:")
      size_str = action_window.get_str(12,15).strip()
      size = int(size_str)
      order_manager.send_new_order(venue, symbol, side, price, size)
       
    elif x in [ord('C'), ord('c')]:
      action_window.erase()
      action_window.border(0)
      action_window.addstr(2,3, "Cancel")
      action_window.addstr(4,3, "Order ID:")
      id_str = action_window.getstr(4, 13).strip()
      order_manager.send_cancel(id_str)
    elif x in [ord('Q'), ord('q')]:
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

