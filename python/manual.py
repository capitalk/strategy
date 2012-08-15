
from os import system
import curses
from market_data import MarketData, Entry
from strategy import Strategy 
import sys 
import atexit 
#def get_param(prompt_string):
#     win2.clear()
#     win2.border(0)
#     win2.addstr(2, 2, prompt_string)
#     win2.refresh()
#     input = win2.getstr(10, 10, 60)
#     return input


md_window = None
action_window = None

def print_market_data(best_bids, best_offers):
  md_window = curses.newwin(40, 70, 2, 52)
  md_window.clear()
  md_window.border(0)
  syms = set(best_bids.keys()).union(set(best_offers.keys()))
  md_window.addstr(2, 3, "Order Book")

  for (i, sym) in enumerate(syms):
    bid_entry = best_bids.get(sym)
    offer_entry = best_offers.get(sym)
    md_window.addstr(4+i*2, 5, "%s : bid = %d @ %s (%s), offer = %d @ %s (%s)" % \
      (sym, bid_entry.size, bid_entry.price, bid_entry.venue, 
            offer_entry.size, offer_entry.price, offer_entry.venue))
  md_window.refresh()
  return md_window

def print_action_menu():
  action_window = curses.newwin(40, 50, 2, 2)
  action_window.clear()
  action_window.border(0)
  action_window.addstr(2,3,"Actions")
  
  action_window.addstr(4,5,"N - New order")
  action_window.addstr(6,5,"C - Cancel order")
  action_window.addstr(8,5,"Q - Quit")
  action_window.refresh()
  return action_window

def ui_update(arg):
  try:
    best_bids = {'USD/JPY':Entry(price = 1.2, size = 10**6, symbol='USD/JPY', venue=1, timestamp=0)} #('price', 'size', 'venue', 'symbol', 'timestamp'))
    best_offers = {'USD/JPY': Entry(price = 1.3, size = 0.5*10**6, symbol = 'USD/JPY', venue = 2, timestamp = 0)}
    md_window = print_market_data(best_bids, best_offers)
    order_window = None
    action_window = print_action_menu()
    action_window.timeout(10)
    x = action_window.getch()

    if x in [ord('P'), ord('p')]:
      print "NEW ORDER"
    elif x in [ord('C'), ord('c')]:
      print "CANCEL"
    elif x in [ord('Q'), ord('q')]:
      curses.endwin()
      exit(0)
      
  except:  
    curses.endwin()
    raise 

STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e023'  


from argparse import ArgumentParser 
parser = ArgumentParser(description='Manual order entry') 
parser.add_argument('--config-server', type=str, default='tcp://*:11111', dest='config_server')
if __name__ == '__main__':
  args = parser.parse_args()
  md = MarketData()
  strategy = Strategy(STRATEGY_ID)
  strategy.connect(args.config_server)
  
  #atexit.register(strategy.close_all)
  strategy.synchronize_market_data(md.update)
  screen = curses.initscr()
  strategy.main_loop(md.update, ui_update)

