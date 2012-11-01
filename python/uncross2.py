#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import logging
import sys
from market_data import MarketData
from strategy_loop import Strategy
from proto_objs.capk_globals_pb2 import BID, ASK
from logging_helpers import create_logger

logger = create_logger('uncross', console_level=logging.DEBUG,
                       console_handler=logging.StreamHandler(sys.stdout),
                       file_name='uncross.log',
                       file_level=logging.DEBUG)

STRATEGY_ID = 'f16e8fc3-846e-43e5-a3bf-8728f42e7972'
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

    # rescue ID only gets set if we're trying to kill the cross
    # after being unequally filled and thus need a new order
    # to get out of the position

        self.rescue_order_id = None
        self.rescue_start_time = None

        self.sent_bid_cancel = False
        self.sent_offer_cancel = False

    def set_rescue_order_id(self, rid):
        self.rescue_order_id = rid
        self.rescue_start_time = time.time()

    def send(self):
        assert not self.sent, "Can't send the same cross twice"

    # send the damn thing

        bid_entry = self.bid_entry
        offer_entry = self.offer_entry
        assert offer_entry.price < bid_entry.price, \
            "Isn't this supposed to be an uncrosser? offer %s not less than bid %s " \
            % (offer_entry.price, bid_entry.price)
        symbol = bid_entry.symbol
        assert symbol == offer_entry.symbol, \
            "Can't uncross two different symbols! (%s, %s)" \
            % (bid_entry.symbol, offer_entry.symbol)

    # NB: Send a BID order to transact with the offer in the order book
    # and vice versa (send an ASK to transact with the available bid)
    #
    # Send an order for the smaller qty, and  presumably more transient, side first
    # Make sure we transact the same amount on both sides
    # KTK Changed criteria to not shoot at single market - bid and ask must be on different markets
    # This is to test msg receipt on FAST

        if bid_entry.size < offer_entry.size:
            smaller_qty = bid_entry.size
            self.offer_order_id = \
                order_manager.send_new_order(bid_entry.venue, symbol,
                    ASK, bid_entry.price, smaller_qty)
            logger.info(
                '(A) Sending ASK (%s): %s, %s, %f, %d',
                self.offer_order_id,
                bid_entry.venue,
                symbol,
                bid_entry.price,
                smaller_qty,
                )
            self.bid_order_id = \
                order_manager.send_new_order(offer_entry.venue, symbol,
                    BID, offer_entry.price, smaller_qty)
            logger.info(
                '(A) Sending BID (%s): %s, %s, %f, %d',
                self.bid_order_id,
                offer_entry.venue,
                symbol,
                offer_entry.price,
                smaller_qty,
                )
        else:
            smaller_qty = offer_entry.size
            self.bid_order_id = \
                order_manager.send_new_order(offer_entry.venue, symbol,
                    BID, offer_entry.price, smaller_qty)
            logger.info(
                '(B) Sending BID (%s): %s, %s, %f, %d',
                self.bid_order_id,
                offer_entry.venue,
                symbol,
                offer_entry.price,
                smaller_qty,
                )
            self.offer_order_id = \
                order_manager.send_new_order(bid_entry.venue, symbol,
                    ASK, bid_entry.price, smaller_qty)
            logger.info(
                '(B) Sending ASK (%s): %s, %s, %f, %d',
                self.offer_order_id,
                bid_entry.venue,
                symbol,
                bid_entry.price,
                smaller_qty,
                )
        self.send_time = time.time()
        self.sent = True

    def send_when_ready(self, wait_time):
        ready_to_send = self.start_time + wait_time >= time.time()
        if ready_to_send:
            self.send()
            logger.info('Sent orders for %s', self)
        else:
            logger.info('Waiting to send orders for %s', self)

    def __str__(self):
        return 'Cross(bid = %s(%s), offer = %s(%s))' % (self.bid_entry,
                self.bid_entry.venue, self.offer_entry,
                self.offer_entry.venue)


def md_update_wrapper(bbo):
    """Update market data and add any changed symbols to 'updated_symbols' set"""

    changed = md.update(bbo)
    if changed:
        updated_symbols.add(bbo.symbol)


def find_best_crossed_pair(min_cross_magnitude, max_size=10 ** 8):
    assert cross is None
    if len(updated_symbols) == 0:
        return
    best_cross = None
    best_cross_magnitude = 0
    for symbol in updated_symbols:
        yen_pair = 'JPY' in symbol
        sorted_bids = md.sorted_bids(symbol)
        sorted_offers = md.sorted_offers(symbol)
        for bid_entry in sorted_bids:
            for offer_entry in sorted_offers:
                price_difference = bid_entry.price - offer_entry.price
                if price_difference <= 0:
                    break
                else:
                    cross_size = min(bid_entry.size, offer_entry.size)
                    cross_magnitude = price_difference * cross_size
                    logger.info(
                        'Cross found: %s (%s %f for %f)@(%s %f for %f) %d FOR %s'
                            ,
                        symbol,
                        bid_entry.venue,
                        bid_entry.price,
                        bid_entry.size,
                        offer_entry.venue,
                        offer_entry.price,
                        offer_entry.size,
                        cross_size,
                        cross_magnitude,
                        )
                    if yen_pair:
                        cross_magnitude /= 80
                    if cross_magnitude > best_cross_magnitude:
                        best_cross = Cross(bid_entry=bid_entry,
                                offer_entry=offer_entry)
                        best_cross_magnitude = cross_magnitude
    if best_cross is not None:
        logger.info('Created cross object: %s', best_cross)
        if best_cross_magnitude < min_cross_magnitude:
            logger.warning('Not sending - cross too small')
            best_cross = None

    # if best_cross.bid_entry.venue == best_cross.offer_entry.venue:
    #  logger.warning("Not sending - venues are the same");
    #  best_cross = None

    updated_symbols.clear()
    return best_cross


def close_unbalanced_cross(bigger, smaller):
    logger.info('Close unbalanced cross, bigger = %s smaller = %s ',
                bigger, smaller)
    order_manager.cancel_if_alive(bigger.id)
    if order_manager.is_alive(smaller.id):
        logger.info('Order %s is alive - attempting to send rescue_order'
                    , smaller.id)

    # KTK NB - if using synthetic cance/replace to modify order the original order may
    # partially filled so the new qty should be bigger.cum_qty - smaller.cum_qty

        rescue_id = order_manager.liquidate_order(md, smaller.id,
                bigger.cum_qty)
        cross.set_rescue_order_id(rescue_id)
    else:
        (side, symbol, venue) = (smaller.side, smaller.symbol,
                                 smaller.venue)

    # none of the smaller side's IDs are alive, so we need to put in a new order

        logger.info(
            'Dead order for %s on %s (side = %s, %d) needs %s',
            symbol,
            venue,
            side,
            smaller.cum_qty,
            bigger.cum_qty,
            )
        price = md.liquidation_price(side, symbol, venue)
        qty_diff = bigger.cum_qty - smaller.cum_qty
        assert qty_diff > 0, \
            "Why did you call close_unbalanced_cross if there's no fill difference?"
        rescue_id = order_manager.send_new_order(venue, symbol, side,
                price, qty_diff)
        logger.info('Liquidation qty = %d, price = %s', qty_diff, price)
        cross.set_rescue_order_id(rescue_id)
    logger.info('Rescue order: %s',
                order_manager.get_order(cross.rescue_order_id))


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

    global cross
    assert cross.sent, "Can't kill a cross before you send it"
    bid = order_manager.get_order(cross.bid_order_id)
    ask = order_manager.get_order(cross.offer_order_id)
    logger.info('kill_cross evaluating: bid=<%s>, ask=<%s>', bid, ask)
    bid_qty = bid.cum_qty
    ask_qty = ask.cum_qty
    if bid.cum_qty == ask.cum_qty:
        logger.info('In kill_cross BID and ASK qty filled are same - checking if either order alive (%s, %s)'
                     % (bid.id, ask.id))
        if not cross.sent_bid_cancel:
            bid_alive = order_manager.cancel_if_alive(bid.id)
            cross.sent_bid_cancel = True
        else:
            logger.info('Not sending bid cancel again')
        if not cross.sent_offer_cancel:
            ask_alive = order_manager.cancel_if_alive(ask.id)
            cross.sent_offer_cancel = True
        else:
            logger.info('Not sending offer cancel again')
        if bid_alive == False and ask_alive == False:
            logger.warning('Both orders dead - finding new cross')

        # sys.exit(-1)
        # cross = find_best_crossed_pair(min_cross_magnitude, max_order_qty)

            both_dead(bid, ask)
    elif bid_qty > ask_qty:
        logger.info('Closing unbalanced cross with bid_qty = %d, ask_qty = %d'
                    , bid_qty, ask_qty)
        close_unbalanced_cross(bid, ask)
    else:
        logger.info('Closing unbalanced cross with bid_qty = %d, ask_qty = %d'
                    , bid_qty, ask_qty)
        close_unbalanced_cross(ask, bid)


def both_dead(bid, offer):
    """Takes two dead orders and optionally places a hedge order if they
     are unevenly filled
  """

    global cross
    logger.info('both_dead(bid=%s, offer=%s)', bid, offer)
    if bid.cum_qty > offer.cum_qty:
        close_unbalanced_cross(bid, offer)
    elif bid.cum_qty < offer.cum_qty:
        close_unbalanced_cross(offer, bid)
    elif bid.cum_qty == 0:
        assert offer.cum_qty == 0
        logger.info('Cross died without any fills')
        cross = None
    else:
        assert offer.avg_price is not None
        assert bid.avg_price is not None
        expected_profit = bid.cum_qty * (offer.price - bid.price)
        profit = bid.cum_qty * (offer.avg_price - bid.avg_price)
        logger.info('Cross completed! Profit expected %s, got %s %s',
                    expected_profit, profit, bid.symbol)
        cross = None


def manage_active_cross(max_order_lifetime):
    global cross

  # logger.info("manage_active_cross(%d)", max_order_lifetime)

    if cross is None:
        logger.warning('manage_active_cross called with cross == none')
    if cross.rescue_order_id:

    # one order got rejected or some other weird situation which
    # required us to hedge against a lopsided position

        order = order_manager.get_order(cross.rescue_order_id)
        rescue_pending = order_manager.is_pending(cross.rescue_order_id)
        rescue_alive = order_manager.is_alive(cross.rescue_order_id)

    # rescue_dead = not (rescue_pending or rescue_alive)

        rescue_expired = time.time() - cross.rescue_start_time >= 10
        logger.info('There is a rescue order: %s', order)

    # if the order is filled, or the rescue has expired,
    # or rescue order has died, give up on it!

        if order.cum_qty == order.qty:
            logger.info('Rescue succeeded: %s' % cross.rescue_order_id)

            cross = None
        else:
            assert rescue_pending or rescue_alive, 'Rescue failed: %s' \
                % order

      # assert (not rescue_expired), "Rescue expired: %s" % order

            if rescue_expired:
                logger.critical('RESCUE EXPIRED!!!!! - maybe cancel/replace failed?'
                                )
            sys.stdout.write('r')
            logger.debug('CURRENT MARKET')
            sorted_bids = md.sorted_bids(order.symbol)
            sorted_offers = md.sorted_offers(order.symbol)
            for b in sorted_bids:
                logger.debug('BID: %s', b)
            for a in sorted_offers:
                logger.debug('ASK: %s', a)
            sys.stdout.flush()
    elif time.time() >= cross.send_time + max_order_lifetime:
        logger.info('Cross expired')
        kill_cross()
    else:

        bid_id = cross.bid_order_id
        bid_alive = order_manager.is_alive(bid_id)
        bid = order_manager.get_order(bid_id)

        offer_id = cross.offer_order_id
        offer_alive = order_manager.is_alive(offer_id)
        offer = order_manager.get_order(offer_id)
        if not (bid_alive or offer_alive):
            logger.info('Both orders dead')
            both_dead(bid, offer)
        elif bid_alive and offer_alive:

      # logger.info("Both orders alive for %s, bid filled = %d, ask filled = %d",
      #  bid.symbol, bid.cum_qty, offer.cum_qty)

            sys.stdout.write('-')
            sys.stdout.flush()
        elif bid_alive and offer.cum_qty < offer.qty:

      # offer is dead with a partial fill

            logger.info('Bid alive, offer dead with %d/%d filled',
                        offer.cum_qty, offer.qty)
            kill_cross()
        elif offer_alive and bid.cum_qty < bid.qty:

      # bid is dead with a partial fill

            logger.info('Offer alive, bid dead with %d/%d filled',
                        offer.cum_qty, offer.qty)
            kill_cross()
        else:
            sys.stdout.write('!')
            sys.stdout.flush()
            logger.info('Still waiting for a fill (bid %d/%d, offer %d/%d)'
                        , bid.cum_qty, bid.qty, offer.cum_qty,
                        offer.qty)


def outgoing_logic(
    min_cross_magnitude,
    new_order_delay=0,
    max_order_lifetime=5,
    max_order_qty=10 ** 6,
    ):

    global cross
    if cross is not None:

    # if not cross.sent:
      # cross.send_when_ready(new_order_delay)
    # else:

        manage_active_cross(max_order_lifetime)

  # this has to come second since the functions above might find the
  # cross is finished and reset the global 'cross' variable to None

    if cross is None:
        cross = find_best_crossed_pair(min_cross_magnitude,
                max_order_qty)

    # if there's no delay, send orders immediately,
    # otherwise it will happen on the next update cycle

        if new_order_delay == 0 and cross is not None:
            cross.send()


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
parser.add_argument('--min-cross-magnitude', type=float, default=0,
                    dest='min_cross_magnitude')
parser.add_argument('--max-order-lifetime', type=float, default=5.0,
                    dest='max_order_lifetime')

import atexit
if __name__ == '__main__':
    args = parser.parse_args()
    strategy = Strategy(STRATEGY_ID)
    order_manager = strategy.connect(args.config_server)


  # TODO: Figure out why zmq sockets hang on exit
  # atexit.register(strategy.close_all)

    def place_orders():
        outgoing_logic(args.min_cross_magnitude, args.order_delay,
                       args.max_order_lifetime, args.max_order_qty)


    logger.info('Started')
    strategy.run(md_update_wrapper, place_orders)
    logger.info('Stopped')

