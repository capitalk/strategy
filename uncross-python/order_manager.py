import uuid
import copy
import time 
from enum import enum 
from fix_constants import ORDER_TYPE, TIME_IN_FORCE

class Status:
  SENT = "sent"
  ALIVE = "alive"
  CANCELED = "canceled"
  REJECTED = "rejected"
  FILLED = "filled"

class Side:
  BID = 0
  OFFER = 1

PendingChange = namedtuple('PendingChange', ('id', 'qty', 'price'))

class Order:
  def __init__(self, venue_id, symbol, side, price, qty,
      order_type =  ORDER_TYPE.LIMIT, time_in_force = TIME_IN_FORCE.DAY, 
      current_id = None):
    
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
    
    self.status = Status.SENT
    
    # when we send a message to the exchange which hasn't yet actually
    # lead to a state change, take not of it in pending_changes 
    # For example: (replace_order_id, )
    self.pending_changes = {}
    self.pending_status = Status.ALIVE
    
    # fill this in when we get ack back from exchange
    self.exchange_status = None
    
    self.last_update_time = time.time()
    if id is None: 
      self.id = str(uuid.uuid4())
    
    
class OrderManager:
  def __init__(self):
    self.orders = {}
    self.live_orders = set([])
  
  def new_order(self, order):
    assert order.id not in self.orders
    assert order.id not in self.live_orders
    self.orders[order.id] = order

  def get_order(self, order_id):
    assert order_id in self.orders, \
      "Couldn't find order id %s" % order_id
    return self.orders[order_id]
    
    
  def cancel(self, order_id, cancel_id):
    """We need the ID of the cancel request itself since we may get future messages 
       about the underlying order but only under the ID of the cancel request"""
    assert order_id in self.live_orders, \
       "Can't cancel %s if it's not in working orders" % order_id
    order = self.get_order(order_id)
    order.status = LocalStatus.CANCELED
    order.exchange_status = ORDER_STATUS.CANCELED
    # if we're canceled nothing else can happen
    order.pending_status = None
    order.pending_changes.clear()
    if order_id in self.sent_orders:
      # canceling a pending new order
      assert order_id not in self.live_orders
      self.sent_orders.remove(order_id)
    elif order_id in self.live_orders:
      assert order_id not in self.sent_orders:
      self.live_orders.remove(order_id)
    else:
      raise RuntimeError('Attempting to cancel order %s which is neither working nor pending' % order_id)
    order.last_update_time = time.time()
    
  def cancel_replace(self, old_order_id, new_order_id, price, qty, unfilled_qty, exchange_status):
    
    assert old_order_id in self.orders, \
      "Couldn't find %s when trying to rename to %s" % (old_order_id, new_order_id)
    assert new_order_id not in self.orders, \
      "Didn't expect %s to already exist when renaming from %s" % (new_order_id, old_order_id)
    
    old_order = self.get(old_order_id)
    # remove this cancel/replace from the pending dict 
    assert new_order_id in old_order.pending_changes, \
      "Received a change from %s -> %s which we never requested!" % (old_order_id, new_order_id)
    del old_order.pending_changes[new_order_id]
    new_order = copy.deepcopy(old_order)
    
    # will remove old_order-id from live_orders
    self.cancel_order(old_order_id)    

    # bring it back to life 
    new_order.id = new_order_id
    new_order.status = Status.ALIVE
    new_order.pending_status = None
    new_order.price = price 
    new_order.qty = qty
    new_order.unfilled_qty = unfilled_qty 
    new_order.exchange_status = exchange_status
    self.live_orders.add(new_order_id)
    self.orders[new_order_id] = new_order
    new_order.last_update_time = time.time()
  
  def partial_fill(self, order_id, filled_qty, unfilled_qty):
    assert order_id in self.live_orders
    assert order_id in self.orders
    
    order = self.orders[order_id]
    assert order.qty == (filled_qty + unfilled_qtty), \
      "Quantities for %s didn't match up: filled(%d) + unfilled(%d) != %d" % \
      (filled_qty, unfilled_qty, order.qty)
      
    order.filled_qty = filled_qty
    order.unfilled_qty = unfilled_qty 
    order.last_update_time = time.time()
  
  
  def handle_execution_report(er):
    order_id = er.cl_order_id
    # only used for cancel and cancel/replace
    original_order_id = er.orig_cl_order_id 

    status = er.order_status
    exec_type = er.exec_type
    
    if exec_type == ORDER_STATUS.REPLACE:
      self.cancel_replace(old_order_id, new_order_id, 
        price = er.price, qty = er.order_qty, unfilled_qty = er.leaves_qty, 
        exchange_status = status)
    elif exec_type in [\
      EXEC_TYPE.STOPPED, 
      EXEC_TYPE.SUSPENDED, 
      EXEC_TYPE.RESTATED, 
      EXEC_TYPE.EXPIRED, 
      EXEC_TYPE.CALCULATED]:
      err_msg = \
      "Unsupported exec_type = %s, order status = %s, order_id = %s, original_order_id = %s" %
        (EXEC_TYPE.to_str(exec_type), EXEC_TYPE.to_str(status), order_id, original_order_id)
      raise RuntimeError(err_msg)
    elif exec_type in [
      EXEC_TYPE.PENDING_NEW, EXEC_TYPE.PENDING_CANCEL, EXEC_TYPE.PENDING_REPLACE]:
      print "Nothing to do for exec_type = %s" % EXEC_TYPE.to_str(exec_type)
    
    
    #DONE_FOR_DAY = '3',
    #CANCELLED  = '4',
    #REPLACE  = '5',
    #REJECTED  = '8',
    #CALCULATED  = 'B',
    
    if status == ORDER_STATUS.NEW:
      assert order_id in order_manager
      order = order_manager[order_id]

        # some ECN's don't tell us about pending changes
        assert order.state in [None, ORDER_STATUS.PENDING_NEW] \
          "Order %d's state got updated to NEW but was previously %s" % (order_id, order.state)
        assert order.size == er.order_qty
        assert order.price == er.price 
        order.state = ORDER_STATUS.NEW
      elif  status == ORDER_STATUS.PARTIAL_FILL:
        assert exec_type != EXEC_TYPE.REPLACE,\
          "BOTH replaced and partial fill: order = %s, original_order = %s" % (order_id, original_order_id)
      elif status = ORDER_STATUS.FILL:
        assert exec_type != EXEC_TYPE.REPLACE, \
          "BOTH filled and replaced: order = %s, original_order = %s" % (order_id, original_order_id)

        order_map_insert_t insert = 
          completedOrders.insert(order_map_value_t(oid, order)); 
            isNewItem = insert.second;
            if (isNewItem) {
                pan::log_DEBUG("Added to completed: ", 
                        pan::blob(oid.get_uuid(), oid.size()));
            }

            // delete from working orders
            order_map_iter_t orderIter = workingOrders.find(oid);
            if (orderIter == workingOrders.end()) {
                pan::log_CRITICAL("OID: ", 
                pan::blob(oid.get_uuid(), oid.size()), 
                " not found in working orders");
            }
            else {
                pan::log_DEBUG("Deleting filled order from working orders");
                workingOrders.erase(orderIter);
            }
        }

        if (ordStatus == capk::ORD_STATUS_CANCELLED) {

            clock_gettime(CLOCK_REALTIME, &ts); 
            pan::log_DEBUG("ORIGOID: ", 
                            pan::blob(origOid.get_uuid(), origOid.size()), 
                            " CLOID: (",pan::blob(oid.get_uuid(), oid.size()),")", 
                            " CANCELLED ", 
                            pan::integer(ts.tv_sec), 
                            ":", 
                            pan::integer(ts.tv_nsec));

            order_map_iter_t orderIter = workingOrders.find(origOid);  
            if (orderIter != workingOrders.end()) {
                pan::log_DEBUG("Deleting order from working orders");
                workingOrders.erase(orderIter);
            }
            else {
                pan::log_WARNING("ORIGOID: ", 
                    pan::blob(origOid.get_uuid(), origOid.size()), 
                    " cancelled but not found in working orders");
                order_map_iter_t pendingIter = pendingOrders.find(origOid);
                if (pendingIter != pendingOrders.end()) {
                    pendingOrders.erase(pendingIter);
                }
                else {
                    pan::log_WARNING("OID: ", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        " cancelled but not found in working OR pending orders");
                }
            }
        }

        // origClOid is the original order that was replaced
        // so now the new order has working order id of clOrdId 
        // with the parameters that were sent in the replace msg
        if (ordStatus == capk::ORD_STATUS_REPLACE) {

            // insert the new order id which is in clOrdId NOT origClOid
            order_map_insert_t insert = 
               workingOrders.insert(order_map_value_t(oid, order)); 

            order_map_iter_t orderIter = workingOrders.find(origOid);
            // orig order must be found in working orders
            assert(orderIter != workingOrders.end());

            // delete the old order id
            workingOrders.erase(orderIter);
        }

        if (ordStatus == capk::ORD_STATUS_PENDING_CANCEL) {
            // We had a partial fill while pending cancel - handle it
            if (order.getExecType() == capk::EXEC_TYPE_PARTIAL_FILL) {
                pan::log_NOTICE("OID: ", pan::blob(origOid.get_uuid(), origOid.size()), 
                        " partial fill while pending cancel");
                completedOrders.insert(order_map_value_t(origOid, order));
            }
            order_map_insert_t insert = 
                    pendingOrders.insert(order_map_value_t(origOid, order));
            isNewItem = insert.second;
            if (isNewItem) {
                pan::log_DEBUG("Added to pending: ",
                            pan::blob(origOid.get_uuid(), origOid.size()));
            }
        }
        if (ordStatus == capk::ORD_STATUS_PENDING_REPLACE) {
            if (order.getExecType() == capk::EXEC_TYPE_PARTIAL_FILL) {
                pan::log_NOTICE("OID: ", pan::blob(origOid.get_uuid(), origOid.size()), 
                        " partial fill while pending replace");
                completedOrders.insert(order_map_value_t(origOid, order));
            }
            order_map_insert_t insert = 
                    pendingOrders.insert(order_map_value_t(origOid, order));
            isNewItem = insert.second;
            if (isNewItem) {
                pan::log_DEBUG("Added to pending: ",
                            pan::blob(origOid.get_uuid(), origOid.size()));
            }


        if (ordStatus == capk::ORD_STATUS_REJECTED) {
          pan::log_DEBUG("Deleting rejected order from pending");
          pendingOrders.erase(orderIter);
  
  def handle_cancel_reject():
    
  

    