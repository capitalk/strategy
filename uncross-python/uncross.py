from proto_objs import spot_fx_md_1_pb2
import gevent 
import zmq 
import time 
import collections
from fix_constants import ORDER_STATUS
import order_engine_constants

context = zmq.Context()

Entry = collections.namedtuple('Entry', ('price', 'size', 'venue', 'timestamp'))

# a pair of entries for bid and offer
Cross = collections.namedtuple('Cross', ('bid', 'offer'))

symbols_to_bids = {} 
# map each symbol (ie USD/JPY) to a dictionary from venue_id's to offer entries
symbols_to_offers = {}
# set of symbols whose data has been updated since the last time the function 
# 'find_best_crossed_pair' ran 
updated_symbols = set([])


"""
message execution_report {
    optional bytes      cl_order_id = 1;
    optional bytes      orig_cl_order_id = 2;
    optional string     exec_id = 3;
    optional sfixed32   exec_trans_type = 4;
    optional sfixed32   order_status = 5;
    optional sfixed32   exec_type = 6;
    optional string     symbol = 7;
    optional string     security_type = 8;
    optional side_t     side = 9;
    optional double     order_qty = 10;
    optional sfixed32   ord_type = 11;
    optional double     price = 12;
    optional double     last_shares = 13;
    optional double     last_price = 14;
    optional double     leaves_qty = 15;
    optional double     cum_qty = 16;
    optional double     avg_price = 17;
    optional sfixed32   time_in_force = 18;
    optional string     transact_time = 19;
    optional string     exec_inst = 20;
    optional sfixed32   handl_inst = 21;
    optional sfixed32   order_reject_reason = 22;
    optional double     min_qty = 23;
    optional sfixed32   venue_id = 24;
    optional string     account = 25;
}
"""

def handle_execution_report(er):
  order_id = er.cl_order_id
  if er.order_status == ORDER_STATUS.NEW:
    assert order_id in order_manager
    order = order_manager[order_id]
    # some ECN's don't tell us about pending changes
    assert order.state in [LOCAL_ORDER_STATUS.SENT, ORDER_STATUS.PENDING_NEW] \
      "Order %d's state got updated to NEW but was previously %s"
    assert order.
    order.state = ORDER_STATUS.NEW
    
      if (isNewItem) {
          pan::log_DEBUG("Added to working: ",
                      pan::blob(oid.get_uuid(), oid.size()));
      }
      size_t numPendingOrders = pendingOrders.erase(oid);
      pan::log_DEBUG("Remaining pending orders: ", pan::integer(numPendingOrders));

      clock_gettime(CLOCK_REALTIME, &ts); 
      pan::log_DEBUG("NEW ",
                      "OID: ", 
                      pan::blob(oid.get_uuid(), oid.size()), 
                      " ", 
                      pan::integer(ts.tv_sec), 
                      ":", 
                      pan::integer(ts.tv_nsec));
  }
"""void 
handleExecutionReport(capkproto::execution_report& er) 
{
  
    timespec ts;
    bool isNewItem;
    capk::Order order;
    order.set(const_cast<capkproto::execution_report&>(er));
    //char oidbuf[UUID_STRLEN];
    uuidbuf_t oidbuf;

    order_id_t oid = order.getOid();
    oid.c_str(oidbuf);
    pan::log_DEBUG("APP Execution report received CLOID: ", oidbuf);

    order_id_t origOid = order.getOrigClOid();
    origOid.c_str(oidbuf);
    pan::log_DEBUG("APP Execution report received ORIGCLOID: ", oidbuf);

    capk::OrdStatus_t ordStatus = order.getOrdStatus();

    //pan::log_DEBUG(er.DebugString());

    // There are three FIX tags that relay the status of an order
    // 1) ExecTransType (20)
    // 2) OrdStatus (39)
    // 3) ExecType (150)
    // Usually OrdStatus == ExecType but the devil lives where they are not
    // equal. For some order statuses they are always the same (e.g. NEW) 
    // so we don't check ExecType but others (e.g. PENDING_CANCEL) they may 
    // be different since the order may exists in more than one state (e.g
    // a fill while a cancel is pending). 
    // see fix-42-with_errata_2001050.pdf on http://fixprotocol.org for more info

    
    if (ordStatus == capk::ORD_STATUS_NEW) {
        assert(workingOrders.find(oid) == workingOrders.end());
        // Can't assert this since not all exchanges send PENDING_NEW before
        // sending ORDER_NEW
        //assert(pendingOrders.find(oid) != pendingOrders.end());

        order_map_insert_t insert = 
                workingOrders.insert(order_map_value_t(oid, order));
        isNewItem = insert.second;
        if (isNewItem) {
            pan::log_DEBUG("Added to working: ",
                        pan::blob(oid.get_uuid(), oid.size()));
        }
        size_t numPendingOrders = pendingOrders.erase(oid);
        pan::log_DEBUG("Remaining pending orders: ", pan::integer(numPendingOrders));

        clock_gettime(CLOCK_REALTIME, &ts); 
        pan::log_DEBUG("NEW ",
                        "OID: ", 
                        pan::blob(oid.get_uuid(), oid.size()), 
                        " ", 
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));
    }

    if (ordStatus == capk::ORD_STATUS_PARTIAL_FILL) {

        if (order.getExecType() == capk::EXEC_TYPE_REPLACE) {
            pan::log_NOTICE("OID: ", pan::blob(origOid.get_uuid(), origOid.size()), 
                    " replaced AND partially filled ");
        }
        order_map_iter_t orderIter = workingOrders.find(origOid);
        // The below assertion will fail (right now) if the strategy receives 
        // an update for an order which is not in its cache. This happens when 
        // strategy crashes and is restarted WITHOUT reading working orders from 
        // persistent storage. 
        if (orderIter == workingOrders.end()) {
            pan::log_CRITICAL("Received PARTIAL FILL for order NOT FOUND in working order cache");
        }
        (*orderIter).second = order;
        completedOrders.insert(order_map_value_t(origOid, order));
    }

    if (ordStatus == capk::ORD_STATUS_FILL) {
       if (order.getExecType() == capk::EXEC_TYPE_REPLACE) {
           pan::log_NOTICE("OID: ", pan::blob(oid.get_uuid(), oid.size()),
                   " replaced AND fully filled");
       }

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

    }
"""
class OrderInfo:
  WAIT = 0 
  NEW = 1
  INSERTED = 2
  def __init__(self, delay = None):
    """
    Status transitions for an order:
    - Upon creation an order is in WAIT, 
      until the current time exceeds self.wait_until
    - Once the order has been placed it switches to NEW
    - If the order gets filled it switches to FILLED
    - 
    """
    self.status = None
    self.wait_until = time.time()
    if delay: 
      self.wait_until += delay
    
    
    
def update_market_data(bbo, market_data):
  print "Symbol", bbo.symbol
  print "Venue", bbo.bb_venue_id
  symbol, venue_id = bbo.symbol, bbo.bb_venue_id  
  new_bid = Entry(bbo.bb_price, bbo.bb_size, venue_id)  
  new_offer = Entry(bbo.ba_price, bbo.ba_size, venue_id)
  
  print "Bid", new_bid
  print "Offer", new_offer
    
  bids = market_data.symbols_to_bids.setdefault(symbol, {})
  old_bid = bids.get(venue_id)
  
  if old_bid != new_bid:
    market_data.updated_symbols.add(symbol)
    bids[venue_id] = new_bid
    
  offers = market_data.symbols_to_offers.setdefault(symbol, {})
  old_offer = offers.get(venue_id)
  if old_offer != new_offer:
    market_data.updated_symbols.add(symbol)
    offers[venue_id] = new_offer
  


pending_cross = None
pending_cross_start_time = None 

      class OrderInfo:
        def __init__(self):
          # at the end of find_best_crossed_pair we are allowd to pick one crossed
          # pair and designate it as a pending order. If the order is still possible 
          # after 5ms then we'll place it 
          self.bid = None
          self.bid_status = None
          self.bid_last_update_time = None

          self.offer = None
          self.offer_status = None
          self.offer_last_update_time = None

def find_best_crossed_pair(market_data, order_state, min_cross_magnitude = 50):
  assert order_state.pending_cross is None
  if len(market_data.updated_symbols) == 0: return
  print "UPDATED SYMBOLS", market_data.updated_symbols
  market_data.updated_symbols.clear()
  best_cross = None
  best_cross_magnitude = min_cross_magnitude
  for symbol, bid_venues) in market_data.symbols_to_bids.iteritems():
    yen_pair = "JPY" in symbol
    offer_venues = market_data.symbols_to_offers[symbol]
    # bids sorted from highest to lowest 
    sorted_bids = sorted(bid_venues, key=lambda (v,e): e.price, reverse=True)
    # offers from lowest to highest
    sorted_offers = sorted(offer_venues, key=lambda (v,e): e.price)
    for (bid_venue, bid_entry) in sorted_bids:
      for (offer_venue, offer_entry) in sorted_offers:
        if bid_entry.price <= offer_entry.price: break
        else:
          min_size = min(bid_entry.size, offer_entry.size)
          price_difference = bid_entry.price - offer_entry.price
          cross_magnitude = price_difference * min_size
          if yen_pair: cross_magnitude /= 80
          if cross_magnitude > best_cross_magnitude:
            best_cross = Cross(bid = bid_entry, offer = offer_entry)
            best_cross_magnitude = cross_magnitude 
            print "Found better cross: ", best_cross
          # even if we're not the best, prefer crosses going between venues
          # to those on a single venue 
          elif best_cross is not None and \
            cross_magnitude > 
            best_cross.bid.venue == best_cross.offer.venue and \
            
      assert len(offer_venues) == len(bid_venues)
      best_bid = None; best_offer = None
      best_bid_size = None; best_offer_size = None
      best_bid_venue = None; best_offer_venue = None
      for (i, (venue_id, entry)) in enumerate(venues.iteritems()):
        bid, offer, bid_size, offer_size = entry 
        if i == 0:
          best_bid = bid; best_offer = offer
          best_bid_size = bid_size; best_offer_size = offer_size
          best_bid_venue = venue_id; best_offer_venue = venue_id
        if bid > best_bid:
            best_bid = bid; best_bid_venue = venue_id
          elif bid == best_bid and bid_size > best_bid_size:
            best_bid_size = bid_size; best_bid_venue = venue_id

          if offer < best_offer:
            best_offer = offer; best_offer_venue = venue_id 
          elif offer == best_offer and offer_size > best_offer_size:
            best_offer_size = offer_size; best_offer_venue = venue_id
        if best_bid > best_offer:
          print symbol, "crossed with bid=", best_bid, "(venue =", best_bid_venue,") and offer =", best_offer, "(venue =", best_offer_venue, ")"
    gevent.sleep(0)

def receive_order_updates(order_addr, order_port):
  socket = make_subscriber(order_addr, order_port)
  while True:
    msg_parts = socket.recv_multipart()
    print msg_parts
    gevent.sleep(0)

STRATEGY_ID = 'f1056929-073f-4c62-8b03-182d47e5e022' 

def say_hello(order_control_socket):
  socket.send_multipart([order_engine_constants.STRATEGY_HELO, STRATEGY_ID])
  [tag, venue_id] = socket.recv_multipart()
  assert tag == order_engine_constants.STRATEGY_HELO_ACK
  return venue_id

def synchronize_market_data(market_data_socket, wait_time):
  print "Synchronizing market data"
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  start_time = time.time()
  while time.time() < start_time + wait_time:
    ready_sockets = dict(poller.poll(1000))
    if ready_sockets.get(market_data_socket) == zmq.POLLIN:
      msg = market_data_socket.recv()
      receive_market_data(msg)
  print "Waited", wait_time, "seconds, entering main loop"

def main_loop(market_data_socket, order_socket):
  poller = zmq.Poller()
  poller.register(market_data_socket, zmq.POLLIN)
  poller.register(order_socket,  zmq.POLLIN|zmq.POLLOUT)
  
  while True:
    ready_sockets = dict(poller.poll(1000))
    if ready_sockets.get(market_data_socket) == zmq.POLLIN:
      print "POLLIN: market data"
      msg = market_data_socket.recv()
      bbo = spot_fx_md_1_pb2.instrument_bbo();
      bbo.ParseFromString(msg)
      update_market_data(bbo)
    if ready_sockets.get(order_socket) == zmq.POLLIN:
      print "POLLIN: order engine"
      msg = order_socket.recv()
      execution_report = SOME_PROTO_BUF
      execution_report.ParseFromString(msg)
      update_order_state(execution_report)
    elif ready_sockets.get(order_socket) == zmq.POLLOUT:
      print "POLLOUT: order engine"
      if updated



from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--market-data', type=str, nargs='+', dest = 'md_addrs')
parser.add_argument('--order-engine', type=str, default='tcp://127.0.0.1', dest='order_addr')
parser.add_argument('--order-engine-control-port', type=int, default=None, dest='order_control_port')
parser.add_argument('--order-engine-port', type=int, default=None, dest='order_port')
parser.add_argument('--startup-wait-time', type=float, default=1, dest='startup_wait_time', 
  help="How many seconds to wait at startup until market data is synchronized")


def init(args):
  md_socket = context.socket(zmq.SUB)
  for addr in args.md_addrs:
     socket.connect(addr)
    md_target = "%s:%s" % (args.md_addrs)
  print "Making socket for", target
 
  if symbols is None:
    print "Subscribing to all messages" 
    socket.setsockopt(zmq.SUBSCRIBE, "")
  
  market_data_socket = make_subscriber(args.md_addr, args.md_port) 
  if args.order_port and args.order_control_port:
    # the order socket is the one through which we send orders and
    # receive execution reports 
    order_socket = receive_order_updates(args.order_addr, args.order_port)
    order_target = "%s:%s" % (args.order_addr, args.order_port)
    print "Connecting Order Engine socket to", order_target
    order_socket.connect(order_target)
    
    # the order control socket is a blocking socket through which we 
    # register our strategy
    order_control_socket = context.socket(zmq.REQ)
    control_target = "%s:%s" % (args.order_addr, args.order_control_port)
    print "Connecting Order Engine control socket to", control_target
    order_control_socket.connect(control_target)
    
    got_ack = say_hello(order_control_socket)
    if not got_ack:
      raise RuntimeError("Couldn't connect to order engine")
  else:
    if args.order_port:
      print "If you give an order engine port you must also give an order engine control port"
    elif args.order_control_port:
      print "If you give an order engine port you must also give an order engine control port"
    order_socket = None
    order_control_socket = None
  

if __name__ == '__main__':
  args = parser.parse_args()
  
  synchronize_with_market(market_data_socket, args.startup_wait_time)
  poll_loop(market_data_socket, order_socket)
  
  
  
    
  #def read_symbols(filename):
  #  f = open(filename)
  #  syms = [] 
  #  for line in f.readlines():
  #    line = line.strip()
  #    if len(line) == 0: continue
  #    elif len(line) != 7 or line[3] != "/": 
  #      raise RuntimeError("Invalid ccy format " + line)
  #    else:
  #      syms.append( line)
  #  f.close()
  #  print syms 
  #  return syms
