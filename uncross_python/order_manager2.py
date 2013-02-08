#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid
import time
import datetime
#from collections import namedtuple
import order_engine_constants
from int_util import int_to_bytes, int_from_bytes
from proto_objs.capk_globals_pb2 import BID, ASK, GTC, GFD, FOK, LIM, \
    MKT
from fix_constants import HANDLING_INSTRUCTION, EXEC_TYPE, \
    EXEC_TRANS_TYPE, ORDER_STATUS
from one_to_many import OneToManyDict

from proto_objs.execution_report_pb2 import execution_report
from proto_objs.order_cancel_pb2 import order_cancel
from proto_objs.order_cancel_reject_pb2 import order_cancel_reject
from proto_objs.new_order_single_pb2 import new_order_single
from proto_objs.order_cancel_replace_pb2 import order_cancel_replace

import venue_attrs

import logging
#from logging_helpers import create_logger

# logger = create_logger('order_manager', file_name = 'order_manager.log', file_level = logging.DEBUG)

logger = logging.getLogger('uncross')


def fresh_id():
    return uuid.uuid4()


def uuid_str(bytes):
    return str(uuid.UUID(bytes=bytes))


class Position:

    def __init__(self, symbol):
        self.symbol = symbol
        self.long_pos = 0
        self.short_pos = 0
        self.long_val = 0.0
        self.short_val = 0.0

    def long_avg_price(self):
        if self.long_pos != 0:
            return self.long_val / self.long_pos
        else:
            return 0.0

    def short_avg_price(self):
        if self.short_pos != 0:
            return self.short_val / self.short_pos
        else:
            return 0.0

    def net_pos(self):
        return self.long_pos - self.short_pos

    def __str__(self):
        return 'Position<symbol=%s long_pos=%f, long_val=%f, long_avg_price=%f short_pos=%f, short_val=%f, short_avg_price=%f net_pos=%f>' \
            % (
            self.symbol,
            self.long_pos,
            self.long_val,
            self.long_avg_price(),
            self.short_pos,
            self.short_val,
            self.short_avg_price(),
            self.net_pos(),
            )


class Order:

    def __init__(
        self,
        order_id,
        venue,
        symbol,
        side,
        price,
        qty,
        order_type=LIM,
        time_in_force=GFD,
        ):

        self.id = order_id

    # Not sure what to do with new Orders--- we give
    # them an id value since it's messy to put None there
    # but really the identifier isn't legitimate until
    # we get a response from the ECN, so I'm also putting
    # the same id as pending

        curr_time = time.time()
        self.creation_time = curr_time
        self.last_update_time = curr_time

        self.venue = venue
        self.symbol = symbol
        self.side = side
        self.price = price
        self.qty = qty
        self.last_price = 0

        self.order_type = order_type
        self.time_in_force = time_in_force

        self.cum_qty = 0

    # will change to full qty if order is accepted

        self.leaves_qty = 0
        self.avg_price = None
        self.last_shares = 0

    # fill this in when we get ack back from exchange

        self.status = None

 # def set_status(self, new_status):
 #   self.status = new_status
 #   self.last_update_time = time.time()

    def __str__(self):
        return 'Order<id = %s, status=%s, venue=%s, side=%s, price=%s, qty=%d, symbol=%s, cum_qty=%d, leaves_qty=%d last_price=%f last_shares=%f LAST UPDATE:%.6f>' \
            % (
            str(self.id),
            ORDER_STATUS.to_str(self.status),
            self.venue,
            self.side,
            self.price,
            self.qty,
            self.symbol,
            self.cum_qty,
            self.leaves_qty,
            self.last_price,
            self.last_shares,
            self.last_update_time,
            )


class OrderManager:

    def __init__(self, strategy_id, order_sockets):
        """order_sockets maps venue ids to zmq DEALER sockets"""

        logger.info('Initializing OrderManager')
        self.orders = {}
        self.live_order_ids = set([])

        # use the strategy id when constructing protobuffers
        self.strategy_id = strategy_id
        self.order_sockets = order_sockets
        self.positions = {}
        self.pending = OneToManyDict()

    def DBG_ORDER_MAP(self):
        logger.debug('******************** <ORDER MAP> *********************'
                     )
        logger.debug('ALIVE:\n')
        for o1 in self.live_order_ids:
            logger.debug('\t%s = %s', o1, self.get_order(o1))
        logger.debug('ORDER:\n')
        for o2 in self.orders:
            logger.debug('\t%s = %s', o2, self.get_order(o2))
        logger.debug('PENDING:\n')
        logger.debug(self.pending.dbg_string())
        logger.debug('******************** </ORDER MAP> *********************'
                     )

  # KTK should use setdefault? E.g. orders.setdefault(order_did, None)

    def get_order(self, order_id):
        assert order_id in self.orders, "Couldn't find order id %s" \
            % str(order_id)
        return self.orders[order_id]

    # def pending_id_accepted(self, order_id, pending_id):
        # logger.info('pending id accepted %s %s', order_id, pending_id)
        # assert self.pending.has_value(pending_id), \
            # 'Unexpected pending ID %s for order %s' % (pending_id,
                # order_id)
        # self.pending.remove_value(pending_id)
        # self.get_order(order_id).id = pending_id
 
    def pending_id_rejected(self, order_id, pending_id):
        logger.info('pending id REJECTED order_id=%s pending_id=%s',
                    order_id, pending_id)
        assert self.pending.has_value(pending_id), \
            'Unexpected pending ID %s for order %s' % (pending_id,
                order_id)
        self.pending.remove_value(pending_id)

    def is_pending(self, pending_id):
        return self.pending.has_value(pending_id)

    def is_alive(self, order_id):
        return order_id in self.live_order_ids

  # def _add_order(self, order):
    # order_id = order.id
    # assert order_id not in self.orders
    # assert order_id not in self.live_order_ids
    # self.orders[order_id] = order
    # # just assume that if we're ever adding a new order that means
    # # it's about to be sent to an ECN-- it doesn't really make sense
    # # to add dead orders
    # self.live_order_ids.add(order_id)

  # def _remove_order(self, order_id):
    # """Remove order from orders & live_order_ids and return """
    # assert order_id in self.orders, "Can't find order %s" % order_id
    # order = self.orders[order_id]
    # del self.orders[order_id]
    # if order_id in self.live_order_ids:
      # self.live_order_ids.remove(order_id)
    # return order

    # KTK TODO removed as dead code 20121106 - seems never to be called from anywhere
    def _rename(self, old_id, new_id):
        logger.debug('_rename %s to %s', old_id, new_id)
        assert old_id in self.orders, \
            "Can't rename non-existent order %s" % str(old_id)
        assert old_id in self.live_order_ids, \
            "Can't rename dead order %s" % str(old_id)

    # WE MIGHT NEED TO REFER TO THE ORDERS BY THEIR OLD NAMES!
    # For now just remove the old IDs from live_order_ids
    # order = self._remove_order(old_id)

        order = self.orders[old_id]
        order.id = new_id
        self.orders[new_id] = order
        if old_id in self.live_order_ids:
            self.live_order_ids.remove(old_id)
            self.live_order_ids.add(new_id)
        return order

  # def _cancel(self, order_id, cancel_id):
    # """We need the ID of the cancel request itself since we may get future messages
       # about the underlying order but only under the ID of the cancel request"""
       #
    # assert order_id in self.live_order_ids, \
       # "Can't cancel %s if it's not in live orders" % order_id
    #
    # self.live_order_ids.remove(order_id)
    #
    # # have to rename the live order first and then kill it once its ID is the
    # # same as that of the cancel request
    # order = self._rename(order_id, cancel_id)
    # order.set_status(ORDER_STATUS.CANCELLED)

    def _update_order(
        self,
        order,
        price,
        qty,
        cum_qty,
        leaves_qty,
        avg_price,
        last_shares,
        last_price,
        status,
        ):

        changed = False
        if order.price != price:
            order.price = price
            changed = True
        if order.last_price != last_price:
            order.last_price = last_price
            changed = True
        if order.qty != qty:
            order.qty = qty
            changed = True
        if order.leaves_qty != leaves_qty:
            order.leaves_qty = leaves_qty
            changed = True
        if avg_price and avg_price > 0 and order.avg_price != avg_price:
            order.avg_price = avg_price
            changed = True

        if order.cum_qty != cum_qty:
            order.cum_qty = cum_qty

    # KTK - could we use last_shares here from ER?
      # old_filled = order.cum_qty
      # old_pos = self.positions.get(order.symbol, 0)

      # if order.side == BID:
        # new_pos = old_pos - old_filled + cum_qty
      # else:
        # new_pos = old_pos + old_filled - cum_qty
      # self.positions[order.symbol] = new_pos
      # order.cum_qty = cum_qty

            changed = True
        if order.last_price != last_price:
            order.last_price = last_price
            changed = True

        if order.last_shares != last_shares:
            order.last_shares = last_shares
            changed = True

        order.status = status

        if changed:
            order.last_update_time = time.time()

    def print_position(self):
        logger.info('POSITION INFO')
        for k in self.positions:
            logger.info('%s', self.positions[k])


    def handle_fill(self, order):
        #logger.debug('HANDLE_FILL(%s)', order)
        pos = self.positions.get(order.symbol, Position(order.symbol))
        if order.side == BID:
            pos.long_pos += order.last_shares
            pos.long_val += order.last_shares * order.last_price
        if order.side == ASK:
            pos.short_pos += order.last_shares
            pos.short_val += order.last_shares * order.last_price

        self.positions[order.symbol] = pos
        self.print_position();

    def _handle_execution_report(self, er):

        cl_order_id = uuid.UUID(bytes=er.cl_order_id)

        # only used for cancel and cancel/replace 
        if er.orig_cl_order_id is not '':
            orig_cl_order_id = uuid.UUID(bytes=er.orig_cl_order_id)
        else:
            logger.warning('ORIG_CL_ORDER_ID IS NOT SET IN THIS ER - using cl_order_id')
            # KTK - is this OK? Fast doesn't fill in orig on new order ack
            orig_cl_order_id = cl_order_id

        venue_id = er.venue_id
        status = er.order_status
        exec_type = er.exec_type
        symbol = er.symbol
        transaction_type = er.exec_trans_type
        side = er.side
        price = er.price
        qty = er.order_qty
        cum_qty = er.cum_qty
        leaves_qty = er.leaves_qty
        avg_price = er.avg_price
        last_price = er.last_price
        last_shares = er.last_shares

        logger.info(
            'Execution: venue_id = %d, cl_order_id = %s, orig_cl_order_id = %s, status = %s, exec_type = %s, transaction_type= %s, side = %s, symbol = %s, price = %f, qty = %d, cum_qty = %d, leaves_qty = %d, avg_price = %f, last_shares= %f, last_price = %f'
                ,
            venue_id,
            cl_order_id,
            orig_cl_order_id,
            EXEC_TYPE.to_str(status),
            EXEC_TYPE.to_str(exec_type),
            transaction_type,
            side,
            symbol,
            price,
            qty,
            cum_qty,
            leaves_qty,
            avg_price,
            last_shares,
            last_price,
            )

    # NEW transactions are updates to our state and STATUS transactions just
    # repeat the values of the most recent transaction. The trickier cases
    # are CANCEL and CORRECT, which refer to the exec_id of the previous
    # transaction they undo or modify via exec_ref_id
    # For now we crash if the ECN tries to cancel or correct a previous
    # transaction

        unsupported_transaction_types = [EXEC_TRANS_TYPE.CANCEL,
                EXEC_TRANS_TYPE.CORRECT, EXEC_TRANS_TYPE.STATUS]
        assert transaction_type not in unsupported_transaction_types, \
            'Unsupported exec_trans_type %s ' \
            % EXEC_TRANS_TYPE.to_str(transaction_type)

    # Catch exec_types which we don't support
    # regardless of the transaction type)

        unsupported_exec_types = [EXEC_TYPE.STOPPED,
                                  EXEC_TYPE.SUSPENDED,
                                  EXEC_TYPE.RESTATED,
                                  EXEC_TYPE.CALCULATED]
        assert exec_type not in unsupported_exec_types, \
            'Unsupported exec_type = %s, order status = %s, cl_order_id = %s, orig_cl_order_id = %s' \
            % (EXEC_TYPE.to_str(exec_type), EXEC_TYPE.to_str(status),
               cl_order_id, orig_cl_order_id)

    # #################################
    #      Update Order fields       #
    # #################################

        assert cl_order_id in self.orders, \
                'Received unknown order: venue=%d, cl_order_id=%s, price=%f, side=%d, qty=%s, filled=%s' \
            % (
            venue_id,
            cl_order_id,
            price,
            side,
            qty,
            cum_qty,
            )

    # Get the order - throws exception if not found

        order = self.get_order(orig_cl_order_id)

    # Update with new fields from ER

        self._update_order(
            order,
            price,
            qty,
            cum_qty,
            leaves_qty,
            avg_price,
            last_shares,
            last_price,
            status,
            )

    # It's possible to get unsolicited cancels and corrects, in which case
    # there is only cl_order_id and an undefined value in orig_cl_order_id
    # cancels and restatements should come in with exec_trans_type = CANCEL
    # or exec_trans_type = CORRECT

        if transaction_type == EXEC_TRANS_TYPE.NEW:
            if exec_type == EXEC_TYPE.NEW:
                assert self.pending.has_value(cl_order_id), \
                    'Received new order for unknown ID <orig_cl_order_id=%s, cl_order_id=%s>' \
                    % (orig_cl_order_id, cl_order_id)

                self.pending.remove_value(cl_order_id)

                self.live_order_ids.add(cl_order_id)
            elif exec_type == EXEC_TYPE.CANCELLED:

                if not self.is_pending(cl_order_id):
                    logger.warning('Unknown cancel for <orig_cl_order_id=%s, cl_order_id=%s> - NOT IN PENDING'
                                   , str(orig_cl_order_id),
                                   str(cl_order_id))
                else:
                    assert orig_cl_order_id in self.live_order_ids, \
                        'Order: %s cancelled not in live orders' \
                        % orig_cl_order_id
                    assert orig_cl_order_id in self.orders, \
                        "Can't rename non-existent order %s" \
                        % str(orig_cl_order_id)
                    assert orig_cl_order_id in self.live_order_ids, \
                        "Can't rename dead order %s" \
                        % str(orig_cl_order_id)

                # WE MIGHT NEED TO REFER TO THE ORDERS BY THEIR OLD NAMES!
                # For now just remove the old IDs from live_order_ids
                # order = self._remove_order(old_id)
                # WE COULD REMOVE FROM ORDERS TOO...
                # del self.orders[orig_cl_order_id]

                    order.id = cl_order_id
                    self.orders[cl_order_id] = order
                    if orig_cl_order_id in self.live_order_ids:
                        self.live_order_ids.remove(orig_cl_order_id)

                    self.pending.remove_value(cl_order_id)
                    self.pending.remove_key(orig_cl_order_id)
            elif exec_type == EXEC_TYPE.FILL:
                logger.info('RECEIVED FILL: %s', order)
                self.handle_fill(order)

                # The check for ord_status == FILL reflects FXCM's STUPID FUCKING HANDLING of
                # ExecType which uses ExecType=FILL even for partial orders so according to
                # the docs if:
                # OrdStatus == PARTIAL_FILL && ExecType==FILL then it's a partial
                # OrdStatus == FILL && ExecType == FILL then it's a full fill
                # FUCK THAT BITCHES

                if status == ORDER_STATUS.FILL:
                    logger.info(" --FILL WAS FULL");
                    self.live_order_ids.remove(cl_order_id)
                elif status == ORDER_STATUS.PARTIAL_FILL:
                    logger.info(" --FILL WAS PARTIAL");
            elif exec_type == EXEC_TYPE.PARTIAL_FILL:
                logger.info('RECEIVED PARTIAL FILL');# %s', order)
                self.handle_fill(order)
            elif exec_type == EXEC_TYPE.REJECTED:

                # NEW order can be rejected (bad price, size, etc...)
                # assert order.id is None

                assert self.pending.has_value(cl_order_id), \
                    'Rejected order not in pending <orig_cl_order_id=%s  cl_order_id=%s' \
                    % (orig_cl_order_id, cl_order_id)
                self.pending.remove_value(cl_order_id)

                # NB - removing the key here will cause all pending messages relating to the order
                # to be removed from pending as well. Thus, if a new order (e.g.) is rejected then
                # the cancel will be rejected as well but not found in pending since the "key" value
                # (i.e. the original rejected order id) has been removed.
                # Sequence is as follows:
                # 1) Send new that will be rejected cl_orde_id = ABC
                # 2) Immediately send cancel on that order so cl_order_id = DEF, orig_cl_order_id = ABC
                # 3) When reject is received key ABC will be removed
                # 4) Removing ABC also removes value DEF from pending map
                # 5) When cancel reject is received the DEF order id is unknown

                self.pending.remove_key(cl_order_id)
            elif exec_type == EXEC_TYPE.PENDING_CANCEL:

                logger.info('RECEIVED PENDING CANCEL <orig_cl_order_id=%s, cl_order_id=%s>'
                            , str(orig_cl_order_id), str(cl_order_id))
            elif exec_type == EXEC_TYPE.REPLACE:

                if not self.is_pending(cl_order_id):
                    logger.warning('Replace not in pending <orig_cl_order_id=%s, cl_order_id=%s>'
                                   , str(orig_cl_order_id),
                                   str(cl_order_id))
                assert orig_cl_order_id in self.live_order_ids, \
                    '%s not in LIVE orders' % str(orig_cl_order_id)
                assert orig_cl_order_id in self.orders, \
                    '%s not in orders' % str(orig_cl_order_id)
                order = self.orders[orig_cl_order_id]
                order.id = cl_order_id
                self.orders[cl_order_id] = order
                if orig_cl_order_id in self.live_order_ids:
                    self.live_order_ids.remove(orig_cl_order_id)
                    self.live_order_ids.add(cl_order_id)

            # print "AFTER REPLACE RECEIVED - does pending contain orig=%s or cl=%s???????????????????????????????" % (orig_cl_order_id, cl_order_id)
            # TODO It's the orig order id that is the key in the pending map - but I don't think this is correct
            # since the request that is pending is the cl_order_id and the order it relates to is the orig_cl_order_id
            # If you do change it then chagne the send_cancel_replace(...) fcn to add to pending correctly as well

                self.pending.remove_key(orig_cl_order_id)
        elif transaction_type == EXEC_TRANS_TYPE.CANCEL:

            logger.warning('Unsolicited CANCEL (busted exec) request on %s'
                           , str(cl_order_id))
            logger.warning('Order was: %s', self.get_order(cl_order_id))
            logger.warning(
                'New exec is: %f, %f, %f, %f, %f, %s',
                price,
                qty,
                cum_qty,
                leaves_qty,
                avg_price,
                status,
                )
        elif transaction_type == EXEC_TRANS_TYPE.CORRECT:

            logger.warning('Unsolicited CORRECT request on %s',
                           str(cl_order_id))
            logger.warning('Order was: %s', self.get_order(cl_order_id))
            logger.warning(
                'New exec is: %f, %f, %f, %f, %f, %s',
                price,
                qty,
                cum_qty,
                leaves_qty,
                avg_price,
                status,
                )

    # ###############################################
    #     Is the order in a terminal state?        #
    # ###############################################

        terminal_states = [ORDER_STATUS.FILL, ORDER_STATUS.CANCELLED,
                           ORDER_STATUS.REJECTED, ORDER_STATUS.EXPIRED]

        if status in terminal_states:
            if cl_order_id in self.live_order_ids:
                logger.warning('Removing %s from live order ids',
                               str(cl_order_id))
                self.live_order_ids.remove(cl_order_id)

      # elif transaction_type == EXEC_TRANS_TYPE.NEW:
      #  logger.warning("Order %s should have been alive before entering terminal state %s", str(cl_order_id), ORDER_STATUS.to_str(status))


    def _handle_cancel_reject(self, cr):
        # cl_order_id is the cancel request id
        cl_order_id = uuid.UUID(bytes=cr.cl_order_id)

        # orig_cl_order_id is the order id of the order we're trying to cancel
        orig_cl_order_id = uuid.UUID(bytes=cr.orig_cl_order_id)

        logger.warning('Cancel reject: cl_order_id = %s, orig_cl_order_id = %s, reason =%s'
                       , cl_order_id, orig_cl_order_id,
                       cr.cancel_reject_reason)
        assert orig_cl_order_id in self.orders, \
            'Cancel reject for unknown original order ID %s' \
            % str(orig_cl_order_id)

        # make sure the cancel is no longer "live" if cl_order_id in self.live_order_ids:
        # N.B. Cancel replace may be rejected and never appear as alive since live orders
        # must have been ack'd at least once (NEW acknowlegement for example) to be moved 
        # into live orders - otherwise they are simply pending
        if self.is_alive(cl_order_id):
            self.live_order_ids.remove(cl_order_id)

        if self.is_pending(cl_order_id):
            self.pending_id_rejected(orig_cl_order_id, cl_order_id)
        else:
            logger.warning('Got unexpected cancel rejection - maybe NEW or REPLACE were rejected or order already cancelled?'
                           )


    def received_message_from_order_engine(self, tag, msg):
        if tag == order_engine_constants.EXEC_RPT:
            er = execution_report()
            er.ParseFromString(msg)
            #logger.debug(er.__str__())
            self._handle_execution_report(er)
        elif tag == order_engine_constants.ORDER_CANCEL_REJ:
            cr = order_cancel_reject()
            #logger.debug(cr.__str__())
            cr.ParseFromString(msg)
            self._handle_cancel_reject(cr)
        else:
            raise RuntimeError('Unsupported order engine message: %s'
                               % order_engine_constants.to_str(tag))

    def _make_cancel_request(self, request_id, order):
        """Takes an order id to cancel, constructs a proto buf which can be sent
       to the order engine.
    """

        pb = order_cancel()
        pb.cl_order_id = request_id.bytes
        pb.orig_order_id = order.id.bytes
        pb.strategy_id = self.strategy_id
        pb.symbol = order.symbol
        pb.side = order.side
        pb.order_qty = order.qty
        return pb

    def _make_new_order_request(self, order):
        order_pb = new_order_single()
        order_pb.order_id = order.id.bytes
        order_pb.strategy_id = self.strategy_id
        order_pb.symbol = order.symbol
        order_pb.side = order.side
        order_pb.order_qty = order.qty
        order_pb.ord_type = order.order_type
        order_pb.price = order.price
        order_pb.time_in_force = order.time_in_force

    # order_pb.account = None

        order_pb.venue_id = order.venue
        return order_pb

    def _make_cancel_replace_request(
        self,
        request_id,
        order,
        price,
        qty,
        ):
        pb = order_cancel_replace()
        pb.orig_order_id = order.id.bytes
        pb.cl_order_id = request_id.bytes
        pb.strategy_id = self.strategy_id

    # hard-coded for baxter-- won't work with FAST or FXCM

        pb.handl_inst = HANDLING_INSTRUCTION.AUTOMATED_INTERVENTION_OK
        pb.symbol = order.symbol
        pb.side = order.side
        pb.order_qty = qty
        pb.price = price
        pb.transact_time = \
            datetime.datetime.utcnow().strftime('%Y%M%D-%H:%M:%S')
        pb.ord_type = order.order_type
        pb.time_in_force = order.time_in_force
        return pb

    def send_new_order(
        self,
        venue,
        symbol,
        side,
        price,
        qty,
        order_type=LIM,
        time_in_force=GFD,
        ):

    # print "Attempting to create new order: venue = %s, symbol = %s, side = %s, price = %s, size = %s" % \
    #  (venue, symbol, side, price, qty)

        order_id = fresh_id()
        logger.info(
            'send_new_order: id=%s, venue=%s, symbol=%s, side=%s, price=%f, size=%s'
                ,
            order_id,
            venue,
            symbol,
            side,
            price,
            qty,
            )
        order = Order(
            order_id,
            venue,
            symbol,
            side,
            price,
            qty,
            order_type=order_type,
            time_in_force=time_in_force,
            )

        pb = self._make_new_order_request(order)
        socket = self.order_sockets[venue]
        tag = int_to_bytes(order_engine_constants.ORDER_NEW)
        bytes = pb.SerializeToString()

        socket.send_multipart([tag, self.strategy_id, order_id.bytes,
                              bytes])

        self.orders[order_id] = order
        self.live_order_ids.add(order_id)
        self.pending.add(order_id, order_id)

        return order_id

    def send_cancel_replace(
        self,
        order_id,
        price,
        qty,
        ):

    # print "Attempting to cancel/replace %s to price=%s qty=%s" % (order_id, price, qty)

        assert order_id in self.orders
        assert order_id in self.live_order_ids
        order = self.orders[order_id]
        assert order.price != price or order.qty != qty, \
            'Trying to cancel/replace without changing anything for order %s' \
            % str(order_id)

        request_id = fresh_id()
        logger.info('send_cancel_replace: orig_id=%s, replace_id=%s price=%s qty=%s'
                     % (order_id, request_id, price, qty))


        pb = self._make_cancel_replace_request(request_id, order,
                price, qty)
        venue = order.venue
        socket = self.order_sockets[venue]
        tag = int_to_bytes(order_engine_constants.ORDER_REPLACE)
        bytes = pb.SerializeToString()
        socket.send_multipart([tag, self.strategy_id, request_id.bytes,
                              bytes])

        self.orders[request_id] = order
        self.pending.add(order_id, request_id)

        return request_id

    def send_synth_cancel_replace(
        self,
        order_id,
        price,
        qty,
        ):
        logger.info('send_synth_cancel_replace: %s to price=%s qty=%s'
                    % (order_id, price, qty))
        assert order_id in self.orders
        assert order_id in self.live_order_ids
        order = self.orders[order_id]
        assert order.price != price or order.qty != qty, \
            'Trying to cancel/replace without changing anything for order %s' \
            % str(order_id)

        # cancel the original order
        cancel_request_id = self.send_cancel(order_id)

        # send the new order with the modified price
        new_order_request_id = self.send_new_order(order.venue,
                order.symbol, order.side, price, qty)

        logger.info('Sent synthetic cancel/replace')
        logger.info('1) Sent cancel to %s: orig_id = %s, new_id = %s',
                    order.venue, str(order_id), str(cancel_request_id))
        logger.info('2) Sent new order to %s: new_id = %s, price = %f, qty= %s'
                    , order.venue, str(new_order_request_id), price,
                    qty)

        return new_order_request_id

    def send_cancel(self, order_id):

        assert order_id in self.orders, 'send_cancel: Unknown order %s' \
            % str(order_id)

        #assert order_id in self.live_order_ids, "send_cancel: Can't cancel dead order %s" % str(order_id)

        order = self.orders[order_id]
        request_id = fresh_id()
        logger.info('Sending cancel for order_id=%s, cancel_request_id=%s'
                    , str(order_id), str(request_id))

        pb = self._make_cancel_request(request_id, order)
        tag = int_to_bytes(order_engine_constants.ORDER_CANCEL)
        bytes = pb.SerializeToString()
        socket = self.order_sockets[order.venue]
        socket.send_multipart([tag, self.strategy_id, request_id.bytes,
                              bytes])

        self.orders[request_id] = order
        self.live_order_ids.add(request_id)
        self.pending.add(order_id, request_id)
        return request_id

    def cancel_if_alive(self, order_id):
        """The cancel method is intentionally dumb and will try to cancel an order
       even if it's in a terminal state. This is the smarter wrapper which 
       first checks if the order is alive and only sends a cancel request
       if it is. 
    """
        
        alive = order_id in self.live_order_ids
        if alive:
            logger.debug("Sending cancel for live order: %s", order_id)
            self.send_cancel(order_id)
        return alive

    def cancel_everything(self):
        for order_id in self.live_order_ids:
            self.send_cancel(order_id)

    def open_orders(self):
        return [self.orders[order_id] for order_id in
                self.live_order_ids]

    def liquidate_all_open_orders(self, md):
        """Takes MarketData, mapping symbol -> venue -> entry,
       replaces all open orders with significantly worse prices
       likely to transact  """

        logger.info('Attempting to liquidate all %d open orders',
                    len(self.live_order_ids))
        request_ids = []
        for order_id in self.live_order_ids:
            request_id = self.liquidate_order(md, order_id)
            request_ids.append(request_id)
        return request_ids

    def liquidate_order(
        self,
        md,
        order_id,
        qty=None,
        ):
        order = self.get_order(order_id)
        qty = (qty if qty is not None else order.qty)
        price = md.liquidation_price(order.side, order.symbol,
                order.venue)
        if venue_attrs.venue_specifics[order.venue].use_synthetic_cancel_replace == True:
            return self.send_synth_cancel_replace(order.id, price=price, qty=qty)
        else:
            return self.send_cancel_replace(order.id, price=price, qty=qty)

