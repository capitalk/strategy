import uuid
import time
import datetime
import order_engine_constants 
from int_util import int_to_bytes, int_from_bytes 
from proto_objs.capk_globals_pb2 import BID, ASK, GTC, GFD, FOK, LIM, MKT 
from fix_constants import HANDLING_INSTRUCTION, EXEC_TYPE, EXEC_TRANS_TYPE, ORDER_STATUS
from collections import namedtuple

from proto_objs.execution_report_pb2 import execution_report
from proto_objs.order_cancel_pb2 import order_cancel
from proto_objs.order_cancel_reject_pb2 import order_cancel_reject
from proto_objs.new_order_single_pb2 import new_order_single
from proto_objs.order_cancel_replace_pb2 import order_cancel_replace
import logging 


"""
There are lots of complicated state transitions in the lifetime of an order 
but we can simplify our model considerably if we focus on three states:
 - Sent but not yet active: 
     order.id in order_manager.live_order_ids and order.status is None
 - Active in the market: 
     order.id in order_manager.live_order_ids and order.status is not None
 - Dead: order.status is not None and 
     order.id not in order_manager.live_order_ids 
"""

def fresh_id():
  return uuid.uuid4().bytes

PendingChange = namedtuple('PendingChange', \
  ('request_id', 'old_id', 'price', 'qty', 'status', 'timestamp'))

class Order:
  def __init__(self, venue, symbol, side, price, qty,
      order_type =  LIM, time_in_force = GFD,  
      id = None):
    
    curr_time = time.time()
    self.creation_time = curr_time
    self.last_update_time = curr_time
    
    if id is None: 
      self.id = fresh_id()
    else:
      self.id = id 
      
  
    self.venue = venue 
    self.symbol = symbol 
    self.side = side 
    self.price = price
    self.qty = qty
    
    self.order_type = order_type
    self.time_in_force = time_in_force
    
    self.filled_qty = 0
    # will change to full qty if order is accepted
    self.unfilled_qty = 0
    
    # fill this in when we get ack back from exchange
    self.status = None
    
  def set_status(self, new_status):
    self.status = new_status
    self.last_update_time = time.time()
         
class OrderManager:
  def __init__(self, strategy_id, order_sockets):
    """order_sockets maps venue ids to zmq DEALER sockets"""
    self.orders = {}
    self.live_order_ids = set([])
    # changes which haven't been applied yet
    self.pending = {}
    # use the strategy id when constructing protobuffers
    self.strategy_id = strategy_id
    self.order_sockets = order_sockets
    self.positions = {}
    
    
  def get_order(self, order_id):
    if order_id in self.orders:
      return self.orders[order_id]
    else:
      raise RuntimeError("Couldn't find order id %s" % order_id)
  
  def is_alive(self, order_id):
    return order_id in self.live_order_ids
    
  def _add_order(self, order):
    order_id = order.id 
    assert order_id not in self.orders
    assert order_id not in self.live_order_ids
    self.orders[order_id] = order
    # just assume that if we're ever adding a new order that means
    # it's about to be sent to an ECN-- it doesn't really make sense
    # to add dead orders 
    self.live_order_ids.add(order_id)
    
    
  def _remove_order(self, order_id):
    """Remove order from orders & live_order_ids and return """
    assert order_id in self.orders, "Can't find order %s" % order_id
    order = self.orders[order_id]
    del self.orders[order_id]
    if order_id in self.live_order_ids:
      self.live_order_ids.remove(order_id)
    return order 
    
  
  def _rename(self, old_id, new_id):
    assert old_id in self.orders, "Can't rename non-existent order %s" % old_id
    assert old_id in self.live_order_ids, "Can't rename dead order %s" % old_id
    assert new_id not in self.orders, \
      "Can't rename %s to %s since new order id already exists" % (old_id, new_id)
   
    # WE MIGHT NEED TO REFER TO THE ORDERS BY THEIR OLD NAMES! 
    # For now just remove the old IDs from live_order_ids 
    #order = self._remove_order(old_id)
    
    order.id = new_id
    self.orders[new_id] = order
    if old_id in self.live_order_ids:
      self.live_order_ids.remove(old_id)
      self.live_order_ids.add(new_id)
    return order 
      
      
  def _cancel(self, order_id, cancel_id):
    """We need the ID of the cancel request itself since we may get future messages 
       about the underlying order but only under the ID of the cancel request"""
       
    assert order_id in self.live_order_ids, \
       "Can't cancel %s if it's not in live orders" % order_id
    
    self.live_order_ids.remove(order_id)

    # have to rename the live order first and then kill it once its ID is the 
    # same as that of the cancel request 
    order = self._rename(order_id, cancel_id)
    order.set_status(ORDER_STATUS.CANCELLED)
    
  def _update_order(self, id, price, qty, filled_qty, unfilled_qty, status):
    order = self.get_order(id)
    changed = False
    if order.price != price:
      order.price = price 
      changed = True
    if order.qty != qty:
      order.qty = qty
      changed = True
    if order.unfilled_qty != unfilled_qty:
      order.unfilled_qty = unfilled_qty 
      changed = True
    
    if order.filled_qty != filled_qty:
      old_filled = order.filled_qty 
      old_pos = self.positions.get(order.symbol, 0) 
      
      if order.side == BID:
        new_pos = old_pos - old_filled + filled_qty
      else:
        new_pos = old_pos + old_filled - filled_qty
      self.positions[order.symbol] = new_pos
      order.filled_qty = filled_qty
      changed = True
    
    if changed: 
      order.last_update_time = time.time() 
  
     
  def _handle_execution_report(self, er):
    order_id = er.cl_order_id
    # only used for cancel and cancel/replace
    original_order_id = er.orig_cl_order_id 

    status = er.order_status
    exec_type = er.exec_type
    transaction_type = er.exec_trans_type
    price = er.price
    qty = er.order_qty
    filled_qty = er.cum_qty 
    unfilled_qty = er.leaves_qty
    
    # NEW transactions are updates to our state and STATUS transactions just
    # repeat the values of the most recent transaction. The trickier cases
    # are CANCEL and CORRECT, which refer to the exec_id of the previous 
    # transaction they undo or modify via exec_ref_id
    # For now we crash if the ECN tries to cancel or correct a previous
    # transaction 
    unsupported_transaction_types = [EXEC_TRANS_TYPE.CANCEL, EXEC_TRANS_TYPE.CORRECT] #EXEC_TRANS_TYPE.NEW, EXEC_TRANS_TYPE.STATUS
    assert transaction_type not in unsupported_transaction_types \
      "Exec transaction type not yet implemented: %s " % \
      EXEC_TRANS_TYPE.to_str(transaction_type)
  
    #  Catch exec_types which we don't support (regardless of the transaction type)
    unsupported_exec_types = [EXEC_TYPE.STOPPED, EXEC_TYPE.SUSPENDED, EXEC_TYPE.RESTATED,  EXEC_TYPE.CALCULATED]
    assert exec_type not in unsupported_exec_types, \
      "Unsupported exec_type = %s, order status = %s, order_id = %s, original_order_id = %s" % \
      (EXEC_TYPE.to_str(exec_type), EXEC_TYPE.to_str(status), order_id, original_order_id)
      
    
    # It's possible to get unsolicited cancels and replaces, in which case
    # there is only order_id and an undefined value in original_order_id
    if transaction_type == EXEC_TRANS_TYPE.NEW and \
        exec_type in [EXEC_TYPE.NEW, EXEC_TYPE.CANCELLED, EXEC_TYPE.REPLACE, EXEC_TYPE.REJECTED]:
      if order_id in self.pending:
        del self.pending[order_id]
      else:
        logging.warning(\
          "Warning: Got unexpected execution report for %s (id = %s, original id = %s)", 
          EXEC_TYPE.to_str(exec_type), order_id, original_order_id)
    
    
    ###########################################################
    #      Rename replaced/canceled orders, kill rejected    #
    ##########################################################
    if transaction_type == EXEC_TRANS_TYPE.NEW and \
        exec_type in [EXEC_TYPE.REPLACE, EXEC_TYPE.CANCELLED]:
      # for now we're not dealing with unsolicited cancels
      assert original_order_id in self.live_order_ids   
      self._rename(original_order_id, order_id)
     
    ##################################
    #      Update Order fields       #
    ##################################
    
    # The logic for which ID to use is messy due to weird interactions
    # between STATUS transactions, cancels/replaces, unsolicited cancels/replaces, 
    # etc...
    # So, we rename the orders above and then assume we can ignore original_order_id.
    # Eventually we should use the _update_order method to also track position. 
    assert order_id in self.orders, \
      "Unknown order id = %s, price = %f, qty = %s, filled = %s" % \
      (order_id, price, qty, filled_qty)
    self._update_order(order_id, price, qty, filled_qty, unfilled_qty, status)
  
    ################################################
    #     Is the order in a terminal state?        #
    ################################################
    terminal_states = [ORDER_STATUS.FILL, ORDER_STATUS.EXPIRED, ORDER_STATUS.CANCELLED, ORDER_STATUS.REJECTED]
    if status in terminal_states:
      if order_id in self.live_order_ids:
        self.live_order_ids.remove(order_id)
      elif transaction_type == EXEC_TRANS_TYPE.NEW:
        logging.warning("Order %s should have been alive before entering terminal state %s", 
          order_id, ORDER_STATUS.to_str(status))
  
  
  def _handle_cancel_reject(self, cr):
    order_id = cr.cl_order_id  
    if order_id in self.pending:
      pending_change = self.pending[order_id]
      assert pending_change.old_id == cr.orig_cl_order_id
      assert pending_change.request_id == order_id
      del self.pending[cr.cl_order_id]
    else:
      print "Got unexepcted cancel rejection: %s" % cr
      
  def received_message_from_order_engine(self, tag, msg):
    if tag == order_engine_constants.EXEC_RPT:
      er = execution_report()
      er.ParseFromString(msg)
      self._handle_execution_report(er)
    elif tag == order_engine_constants.ORDER_CANCEL_REJ:
      cr = order_cancel_reject()
      cr.ParseFromString(msg)
      self._handle_cancel_reject(cr)
    else:
      raise RuntimeError("Unsupported order engine message: %s" % order_engine_constants.to_str(tag))
  
  def _make_cancel_request(self, request_id, order):
    """Takes an order id to cancel, constructs a proto buf which can be sent
       to the order engine.
    """
    pb = order_cancel()
    pb.cl_order_id = request_id
    pb.orig_order_id = order.id
    pb.strategy_id = self.strategy_id
    pb.symbol = order.symbol
    pb.side = order.side
    pb.order_qty = order.qty
    return pb
    
  def _make_new_order_request(self, order):
    order_pb = new_order_single()
    order_pb.order_id = order.id
    order_pb.strategy_id = self.strategy_id
    order_pb.symbol = order.symbol
    order_pb.side = order.side 
    order_pb.order_qty = order.qty 
    order_pb.ord_type = order.order_type
    order_pb.price = order.price
    order_pb.time_in_force = order.time_in_force
    # order_pb.account = None
    order_pb.venue = order.venue
    return order_pb
    
  def _make_cancel_replace_request(self, request_id, order, price, qty):
    pb = order_cancel_replace()
    pb.orig_order_id = order.id
    pb.cl_order_id = request_id
    pb.strategy_id = self.strategy_id
    # hard-coded for baxter-- won't work with FAST or FXCM
    pb.handl_inst = HANDLING_INSTRUCTION.AUTOMATED_INTERVENTION_OK
    pb.symbol = order.symbol
    pb.side = order.side 
    pb.order_qty = order.qty 
    pb.price = order.price 
    pb.transact_time = datetime.datetime.utcnow().strftime('%Y%M%D-%H:%M:%S')
    pb.ord_type = order.order_type
    pb.time_in_force = order.time_in_force
    return pb 
    
    
  def send_new_order(self, venue, symbol, side, price, qty, order_type = LIM, time_in_force = GFD):
    #print "Attempting to create new order venue = %s, symbol = %s, side = %s, price = %s, size = %s" % \
    #  (venue, symbol, side, price, qty)
    
    order_id = fresh_id()
    order = Order(venue, symbol, side, price, qty, id = order_id,
      order_type = order_type, time_in_force = time_in_force)
    self.orders[order_id] = order
    self.live_order_ids.add(order_id)

    change = PendingChange(old_id = None, request_id = order_id, 
      status = ORDER_STATUS.NEW, price = price, qty = qty, 
      timestamp = time.time())
    self.pending[order_id] = change
    pb = self._make_new_order_request(order)
    socket = self.order_sockets[venue]
    tag = int_to_bytes(order_engine_constants.ORDER_NEW)
    bytes = pb.SerializeToString()
    socket.send_multipart([tag, self.strategy_id, order_id, bytes])
    logging.debug("Sent new order to %s: %s", venue, order)
    return order_id
 
  def send_cancel_replace(self, order_id, price, qty):
    #print "Attempting to cancel/replace %s to price=%s qty=%s" % (order_id, price, qty)
    assert order_id in self.orders
    assert order_id in self.live_order_ids
    order = self.orders[order_id]
    assert order.price != price or order.qty != qty, \
      "Trying to cancel/replace without changing anything for order %s" % order-id
    
    request_id = fresh_id()
    change = \
      PendingChange(old_id = order_id, request_id = request_id,
        price = price, qty = qty, status = None, timestamp = time.time())
    self.pending[request_id] = change
    
    pb = self._make_cancel_replace_request(request_id, order, price, qty)
    venue = order.venue 
    socket = self.order_sockets[venue]
    tag = int_to_bytes(order_engine_constants.ORDER_REPLACE)
    bytes = pb.SerializeToString()
    socket.send_multipart([tag, self.strategy_id, request_id, bytes])

    logging.debug(\
     "Sent cancel/replace to %s: orig_id = %s, new_id = %s, price = %s, qty= %s",
      venue, order_id, request_id, price, qty)
    return request_id
    
  def send_cancel(self, order_id):
    #print "Attempting to cancel order %s" % order_id
    
    assert order_id in self.orders, "Unknown order %s" % order_id
    assert order_id in self.live_order_ids, "Can't cancel dead order %s" % order_id
    order = self.orders[order_id]
    request_id = fresh_id()
    change = PendingChange(old_id=order_id, 
      request_id=request_id, status = ORDER_STATUS.CANCELLED, 
      price = None, qty = None, timestamp = time.time())
    self.pending[request_id] = change

    pb = self._make_cancel_request(request_id, order)
    tag = int_to_bytes(order_engine_constants.ORDER_CANCEL)
    bytes = pb.SerializeToString()
    socket = self.order_sockets[order.venue]
    socket.send_multipart([tag, self.strategy_id, request_id, bytes])
    logging.debug("Sent cancel to %s: order_id = %s", order_id)
    return request_id
  
  def cancel_if_alive(self, order_id):
    """The cancel method is intentionally dumb and will try to cancel an order
       even if it's in a terminal state. This is the smarter wrapper which 
       first checks if the order is alive and only sends a cancel request
       if it is. 
    """
    alive = order_id in self.live_order_ids 
    if alive: self.send_cancel(order_id)
    return alive 
    
  def cancel_everything(self):
    for order_id in self.live_order_ids:
      self.send_cancel(order_id)
    
  def open_orders(self):
    return [self.orders[order_id] for order_id in self.live_order_ids]
  
  
  def liquidate_all_open_orders(self, md):
    """Takes MarketData, mapping symbol -> venue -> entry,
       replaces all open orders with significantly worse prices
       likely to transact  """
   
    logging.debug("Attempting to liquidate all %d open orders", len(self.live_order_ids))
    request_ids = []
    for order_id in self.live_order_ids:
      request_id = self.liquidate_order(md, order_id)
      request_ids.append(request_id)
    return request_ids
    
  def liquidate_order(self, md, order_id, qty = None):
    order = self.get_order(order_id)
    qty = (qty if qty is not None else order.qty)
    price = md.liquidation_price(order.side, order.symbol, order.venue)
    return self.send_cancel_replace(order.id, price = price, qty = qty)
    
    