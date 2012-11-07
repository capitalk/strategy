#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import logging
import sys
from market_data import MarketData
from strategy_loop import Strategy
from proto_objs.capk_globals_pb2 import BID, ASK
from logging_helpers import create_logger
import order_constants

logger = create_logger('market_test', console_level=logging.DEBUG,
                       console_handler=logging.StreamHandler(sys.stdout),
                       file_name='market_test.log',
                       file_level=logging.DEBUG)

# Original

STRATEGY_ID = '46210d2f-61b0-48fe-be15-4cdf17b14b48'

# Uncrosser's ID
# STRATEGY_ID = 'f16e8fc3-846e-43e5-a3bf-8728f42e7972'

md = MarketData()

# we get an order_manager back from Strategy.connect

order_manager = None
updated_symbols = set([])

bid = None
ask = None
bid_id = 0
ask_id = 0
replace_id = 0
bid_in_market = False
ask_in_market = False
send_time_1 = 0
send_time_2 = 0
send_time_3 = 0
send_time_4 = 0
send_time_5 = 0
send_time_6 = 0
cancel_sent = False


def reset_global_state():
    global bid
    global ask
    global bid_id
    global ask_id
    global replace_id
    global bid_in_market
    global ask_in_market
    global send_time_1
    global send_time_2
    global send_time_3
    global send_time_4
    global send_time_5
    global send_time_6
    bid = None
    ask = None
    bid_id = 0
    ask_id = 0
    replace_id = 0
    bid_in_market = False
    ask_in_market = False
    send_time_1 = 0
    send_time_2 = 0
    send_time_3 = 0
    send_time_4 = 0
    send_time_5 = 0
    send_time_6 = 0
    cancel_sent = False


def md_update_wrapper(bbo):
    """Update market data and add any changed symbols to 'updated_symbols' set"""

    changed = md.update(bbo, False)
    if changed:
        updated_symbols.add(bbo.symbol)


def test_reject_new(symbol='EUR/USD'):
    global bid_in_market
    global updated_symbols

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new')
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                BID, best_bid.price - 0.005, 999999999999)
        t1 = time.time()
        logger.info('Finished new, sending cancel')
        order_manager.send_cancel(bid_id)
        t2 = time.time()
        logger.info('Finished cancel')
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('Send cancel took: %f' % (t2 - t1))
        logger.info('*********')
        bid_in_market = True


def test_reject_cancel_replace(symbol='EUR/USD'):
    global bid_in_market
    global updated_symbols
    global replace_id
    global send_time_1
    global cancel_sent
    global bid_id

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    # print "%f@%f" % (best_bid.price, best_ask.price)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new')
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                BID, best_bid.price - 0.005, 875000)
        t1 = time.time()
        logger.info('Finished new, sending replace')
        replace_id = order_manager.send_cancel_replace(bid_id,
                best_bid.price - 0.003, 9999999999999)
        t2 = time.time()
        logger.info('Finished replace')
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('Send cancel took: %f' % (t2 - t1))
        logger.info('*********')
        send_time_1 = time.time()
        bid_in_market = True
    if bid_in_market == True and cancel_sent == False:
        if time.time() - send_time_1 > 2:
            order_manager.send_cancel(bid_id)
            cancel_sent = True


def test_cancel_replace(symbol='EUR/USD'):
    global bid_in_market
    global updated_symbols
    global replace_id
    global send_time_1
    global cancel_sent

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    # print "%f@%f" % (best_bid.price, best_ask.price)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new')
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                BID, best_bid.price - 0.005, 875000)
        t1 = time.time()
        logger.info('Finished new, sending replace')
        replace_id = order_manager.send_cancel_replace(bid_id,
                best_bid.price - 0.003, 250000)
        t2 = time.time()
        logger.info('Finished replace')
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('Send replace took: %f' % (t2 - t1))
        logger.info('*********')
        send_time_1 = time.time()
        bid_in_market = True
    if bid_in_market == True and cancel_sent == False:
        if time.time() - send_time_1 > 2:
            order_manager.send_cancel(replace_id)
            cancel_sent = True


# BUY the BA on the book

def test_hit_ask_single(symbol='EUR/USD', qty=1000000):
    global ask_in_market
    global updated_symbols

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if ask_in_market == False:
        t0 = time.time()
        logger.info('Sending new bid at current bestk ask: %f',
                    best_ask.price)
        bid_id = order_manager.send_new_order(best_ask.venue, symbol,
                BID, best_ask.price, qty)
        t1 = time.time()
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('*********')
        ask_in_market = True


def send_bid(
    venue_id,
    price,
    symbol='EUR/USD',
    qty=666000,
    ):
    global ask_in_market
    global updated_symbols

    if ask_in_market == False:
        logger.info('Not waiting for market update just sending bid')
        t0 = time.time()
        logger.info('Sending new bid at: %f', price)
        ask_id = order_manager.send_new_order(venue_id, symbol, BID,
                price, qty)
        t1 = time.time()
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('*********')
        ask_in_market = True


def send_offer(
    venue_id,
    price,
    symbol='EUR/USD',
    qty=666000,
    ):
    global bid_in_market
    global updated_symbols

    if bid_in_market == False:
        logger.info('Not waiting for market update just sending offer')
        t0 = time.time()
        logger.info('Sending new ask at: %f', price)
        bid_id = order_manager.send_new_order(venue_id, symbol, ASK,
                price, qty)
        t1 = time.time()
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('*********')
        bid_in_market = True


# SELL the BB on the book

def test_hit_bid_single(symbol='EUR/USD', qty=1000000):
    global bid_in_market
    global updated_symbols

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new ask at current bestk ask: %f',
                    best_bid.price)
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                ASK, best_bid.price, qty)
        t1 = time.time()
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('*********')
        bid_in_market = True


def test_partial_fill_bid():
    if bid_in_market == False:
        logger.debug('testing partial bill - BID side')
        test_hit_bid_single(10000000)


def test_partial_fill_ask():
    if bid_in_market == False:
        logger.debug('testing partial bill - BID side')
        test_hit_ask_single(10000000)


def test_cancel_replace_with_partials(symbol='EUR/USD'):
    global bid_in_market
    global updated_symbols

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new')
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                BID, best_bid.price - 0.005, 875000)
        t1 = time.time()
        logger.info('Finished new, sending cancel')
        order_manager.send_cancel_replace(bid_id, best_bid.price,
                5555000)
        t2 = time.time()
        logger.info('Finished cancel')
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('Send cancel took: %f' % (t2 - t1))
        logger.info('*********')
        bid_in_market = True


def cancel_order_id(id):
    logger.debug('Cancelling order: %s', id)
    cancel_id = order_manager.send_cancel(id, bypass=True)


def test_cancel(symbol='EUR/USD'):
    global bid_in_market
    global updated_symbols

    # Avoids "key error" when looking up symbols in MD array if they have
    # not been updated

    if symbol not in updated_symbols:
        return

    best_bid = md.get_bid(symbol)
    best_ask = md.get_offer(symbol)

    if best_bid.price == order_constants.NO_BID or best_ask.price \
        == order_constants.NO_ASK:
        print 'Either bid or ask unavailable - invalid market'
        return

    if bid_in_market == False:
        t0 = time.time()
        logger.info('Sending new')
        bid_id = order_manager.send_new_order(best_bid.venue, symbol,
                BID, best_bid.price - 0.005, 875000)
        t1 = time.time()
        logger.info('Finished new, sending cancel')
        order_manager.send_cancel(bid_id)
        t2 = time.time()
        logger.info('Finished cancel')
        logger.info('*********')
        logger.info('Send new took: %f' % (t1 - t0))
        logger.info('Send cancel took: %f' % (t2 - t1))
        logger.info('*********')
        bid_in_market = True


    # self.offer_order_id = \
    #     order_manager.send_new_order(bid_entry.venue, symbol, ASK,
    #       bid_entry.price+0.001, smaller_qty)
    #   logger.info("(A) Sending ASK (%s): %s, %s, %f, %d", self.offer_order_id, bid_entry.venue, symbol, bid_entry.price, smaller_qty);
    #   self.bid_order_id = \
    #     order_manager.send_new_order(offer_entry.venue, symbol, BID,
    #       offer_entry.price-0.001, smaller_qty)
    #   logger.info("(A) Sending BID (%s): %s, %s, %f, %d", self.bid_order_id, offer_entry.venue, symbol, offer_entry.price, smaller_qty);

def test():

    # test_reject_new()
    # test_cancel()
    # test_cancel_replace()
    # test_reject_cancel_replace()
    # test_hit_bid_single(symbol="EUR/USD")
    # test_hit_ask_single()
    # test_partial_fill_bid()
    # test_cancel_replace_with_partials()

    #send_bid(venue_id=327878, price=1.2050, symbol='EUR/CHF')
    send_offer(venue_id=327878, price=1.33, symbol='EUR/USD')


    #send_offer(venue_id=327878, price=1.2070, symbol='EUR/CHF')
    send_bid(venue_id=327878, price=1.20, symbol='EUR/USD')

from argparse import ArgumentParser
parser = ArgumentParser(description='Market uncrosser')
parser.add_argument('--config-server', type=str, default='tcp://*:11111'
                    , dest='config_server')
parser.add_argument('--max-order-qty', type=int, default=10 ** 8,
                    dest='max_order_qty')
parser.add_argument('--order-delay', type=float, default=0.0,
                    dest='order_delay',
                    help='How many milliseconds should I delay orders by?'
                    )
parser.add_argument('--startup-wait-time', type=float, default=1,
                    dest='startup_wait_time',
                    help='How many seconds to wait at startup until market data is synchronized'
                    )
parser.add_argument('--min-cross-magnitude', type=float, default=35,
                    dest='min_cross_magnitude')
parser.add_argument('--max-order-lifetime', type=float, default=5.0,
                    dest='max_order_lifetime')
parser.add_argument('--oid', type=str, dest='single_order_id')

import atexit
if __name__ == '__main__':
    args = parser.parse_args()
    strategy = Strategy(STRATEGY_ID)
    order_manager = strategy.connect(args.config_server)

  # TODO: Figure out why zmq sockets hang on exit
  # atexit.register(strategy.close_all)

    reset_global_state()
    if args.single_order_id is not None:
        cancel_order_id(args.single_order_id)


    def place_orders():
        test()

    logger.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Started')
    strategy.run(md_update_wrapper, place_orders, order_first=True)
    logger.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Stopped')

