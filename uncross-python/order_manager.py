import uuid
import time
import order_engine_constants 
from fix_constants import ORDER_TYPE, TIME_IN_FORCE
from fix_constants import EXEC_TYPE, EXEC_TRANS_TYPE, ORDER_STATUS
from collections import namedtuple

from exec_report_pb2 import execution_report
from order_cancel_pb2 import order_cancel
from order_cancel_rej_pb2 import order_cancel_reject
from new_order_single_pb2 import new_order_single

class Side:
  BID = 1
  OFFER = 2

"""
There are lots of complicated state transitions in the lifetime of an order 
but we can simplify our model considerably if we focus on three states:
 - Sent but not yet active: 
     order.id in order_manager.live_orders and order.status is None
 - Active in the market: 
     order.id in order_manager.live_orders and order.status is not None
 - Dead: order.status in not None and
    order.id in order_manager.dead_orders  
Additionally, instead of treating each distinct order id as a distinct order
we instead treat the chain of order ids as versions of the same order-- 
so keep just one order object around but update its ID and properties. 
"""

def fresh_id():
  return str(uuid.uuid4())

class Order:
  def __init__(self, venue_id, symbol, side, price, qty,
      order_type =  ORDER_TYPE.LIMIT, time_in_force = TIME_IN_FORCE.DAY, 
      id = None):
      
    self.last_update_time = time.time()

    if id is None: 
      self.id = fresh_id()
    else:
      self.id = id 
      
  
    self.venue_id = venue_id 
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

Fill = namedtuple('Fill', ('symbol', 'venue', 'price', 'qty'))
PendingChange = namedtuple('PendingChange', \
   ('old_id', 'new_id', 'field', 'old_value', 'new_value', 'timestamp'))
   
class OrderManager:
  def __init__(self, strategy_id):
    self.orders = {}
    self.live_order_ids = set([])
    self.pending = {}
    # use the strategy id when constructing protobuffers
    self.strategy_id = strategy_id
    
    
  def get_order(self, order_id):
    if order_id in self.orders:
      return self.orders[order_id]
    else:
      raise RuntimeError("Couldn't find order id %s" % order_id)
    
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
    was_live = old_id in self.live_order_ids
    order = self._remove_order(old_id)
    order.id = new_id
    self.orders[new_id] = order
    if was_live:
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
    order.status = ORDER_STATUS.CANCELLED
    self.last_update_time = time.time()  
    
  def _update_order(self, id, price, qty, filled_qty, unfilled_qty, status):
    order = self.get_order(id)
    changed = False
    if order.price != price:
      order.price = price 
      changed = True
    if order.qty != qty:
      order.qty = qty
      changed = True
    if order.filled_qty != filled_qty:
      order.filled_qty = filled_qty
      changed = True
    if order.unfilled_qty != unfilled_qty:
      order.unfilled_qty = unfilled_qty 
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
    assert transaction_type in [EXEC_TRANS_TYPE.NEW, EXEC_TRANS_TYPE.STATUS], \
      "Exec transaction type not yet implemented: %s " % \
      EXEC_TRANS_TYPE.to_str(transaction_type)
    
    
    # It's possible to get unsolicited cancels and replaces, in which case
    # there is only order_id and an undefined value in original_order_id
    
    if transaction_type == EXEC_TRANS_TYPE.NEW and \
        exec_type in [EXEC_TYPE.NEW, EXEC_TYPE.CANCEL, EXEC_TYPE.REPLACE, EXEC_TYPE.REJECTED]:
      if order_id in self.pending_changes:
        del self.pending_changes[order_id]
      else:
        print "Warning: Got unexpected execution report for %s (id = %s, original id = %s)" % \
          (EXEC_TYPE.to_str(exec_type), order_id, original_order_id)
      
    
    ###################################################
    #    Catch exec_types which we don't support      #
    ##################################################
    if exec_type in [EXEC_TYPE.STOPPED, EXEC_TYPE.SUSPENDED, EXEC_TYPE.RESTATED,  EXEC_TYPE.CALCULATED]:
      err_msg = \
        "Unsupported exec_type = %s, order status = %s, order_id = %s, original_order_id = %s" % \
         (EXEC_TYPE.to_str(exec_type), EXEC_TYPE.to_str(status), order_id, original_order_id)
      raise RuntimeError(err_msg)
    
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
    assert original_order_id not in self.orders
    assert order_id in self.orders
    self._update_order(order_id, price, qty, filled_qty, unfilled_qty, status)
  
    ################################################
    #     Is the order in a terminal state?        #
    ################################################
    if status in [ORDER_STATUS.FILL, ORDER_STATUS.EXPIRE, ORDER_STATUS.CANCELLED, ORDER_STATUS.REJECTED ]:
      if order_id in self.live_order_ids:
        self.live_order_ids.remove(order_id)
  
  
  
  def _handle_cancel_reject(self, cr):
    order_id = cr.cl_order_id  
    if order_id in self.pending:
      pending_change = self.pending[order_id]
      assert pending_change.old_id == cr.orig_cl_order_id
      assert pending_change.new_id == order-id
      del self.pending[cr.cl_order_id]
    else:
      print "Got unexepcted cancel rejection: %s" % cr
      
  def received_message_from_order_engine(self, tag, msg):
    if tag == order_engine_constants.EXC_RPT:
      er = execution_report()
      er.ParseFromString(msg)
      self._handle_execution_rport(er)
    elif tag == order_engine_constants.ORDER_CANCEL_REJ:
      cr = order_cancel_reject()
      cr.ParseFromString(msg)
      self._handle_cancel_reject(cr)
    else:
      raise RuntimeError("Unsupported order engine message: %s" % order_engine_constants.to_str(tag))
  
  def make_cancel_request(self, order_id, strategy_id):
  """Takes an order id to cancel, constructs a proto buf which can be sent
     to the order engine. As a side-effect, adds the new cancel-request ID
     to pending changes. 
  """
  assert order_id in self.orders, "Unknown order %s" % order_id
  assert order_id in self.live_order_ids, "Can't cancel dead order %s" % order_id
  
  cr_id = fresh_id()
  
  order = self.orders[order_id]
  
  cr = cancel_request()
  cr.cl_order_id = cr_id
  cr.orig_order_ order_id
  cr.strategy_id = self.strategy_id
  cr.symbol = order.symbol
  cr.side = order.side
  cr.order_qty = order.qty
  
  self.pending[cr_id] = PendingChange(old_id=order_id, new_id=cr_id, 
    field='status', old_value=order.status, new_value= ORDER_STATUS.CANCELLED, 
    timestamp = time.time())
  return cr
  
  def make_new_order(self, venue, symbol, side, price, qty, 
       order_type =  ORDER_TYPE.LIMIT, time_in_force = TIME_IN_FORCE.DAY):
    order_id = fresh_id()
    order = Order(venue, symbol, side, price, qty, id = order_id,
      order_type = order_type, time_in_force = time_in_force)
    self.orders[order_id] = order
    self.live_order_ids.append(order_id)
    
    pending_change = PendingChange(old_id = None, new_id = order_id, 
      field='status', old_value=None, new_value = ORDER_STATUS.NEW, 
      timestamp = time.time())
    self.pending_changes[order_id] = pending_change
    
    # send this probobuf back to order engine to actually place the order
    order_pb = new_order_single()
    order_pb.order_id = order_id
    order_pb.strategy_id = self.strategy_id
    order_pb.symbol = symbol
    order_pb.side = side 
    order_pb.order_qty = qty 
    order_pb.ord_type = order_type
    order_pb.price = price
    order_pb.time_in_force = time_in_force
    #order_pb.account = None
    order_pb.venue_id = venue
    return order_pb
    