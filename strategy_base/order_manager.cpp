#include "order_manager.h"


#include <zmq.hpp>
#include <signal.h>


#include "google/dense_hash_map"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>

#include <uuid/uuid.h>

#include "utils/msg_types.h"
#include "utils/order_constants.h"
#include "utils/logging.h"
#include "utils/timing.h"
#include "utils/time_utils.h"
#include "utils/bbo_book_types.h"
#include "utils/types.h"
#include "utils/venue_globals.h"

using google::dense_hash_map;

namespace capk {

// Hash tables and typedefs for storing order states
typedef order_map_t::iterator order_map_iter_t;
typedef std::pair<order_map_iter_t, bool> order_map_insert_t;
typedef std::pair<order_id_t, capk::Order> order_map_value_t;

typedef dense_hash_map<order_id_t, Order, std::tr1::hash<order_id>, eq_order_id> order_map_t;
order_map_t pendingOrders;
order_map_t workingOrders;
order_map_t completedOrders;	

void list_orders();

//capkproto::new_order_single
void
create_order(capkproto::new_order_single* nos, 
                const strategy_id_t& sid,
                const char* symbol, 
				capk::Side_t side,
				double quantity,
				double price) 
{
    assert(nos);
#ifdef LOG
	pan::log_DEBUG("create_order()");
#endif
	//capkproto::new_order_single nos;
	nos->set_strategy_id(sid.get_uuid(), sid.size());
	nos->set_symbol(symbol);
	if (side == capk::BID) {
		nos->set_side(capkproto::BID);
	}
	else if (side == capk::ASK) {
		nos->set_side(capkproto::ASK);
	}
	nos->set_order_qty(quantity);
	nos->set_price(price);

	nos->set_ord_type(capkproto::LIM);
	nos->set_time_in_force(capkproto::GFD);
	//return nos;
}	


void 
handleExecutionReport(capkproto::execution_report& er) 
{
    //pan::log_DEBUG("handleExecutionReport()");
    // turn the er into an order object
    timespec ts;
    bool isNewItem;
    capk::Order order;
    order.set(const_cast<capkproto::execution_report&>(er));

    order_id_t oid = order.getOid();

#ifdef LOG
    uuidbuf_t oidbuf;
    oid.c_str(oidbuf);
    pan::log_DEBUG("APP Execution report received CLOID: ", oidbuf);
#endif

    order_id_t origOid = order.getOrigClOid();

#ifdef LOG
    origOid.c_str(oidbuf);
    pan::log_DEBUG("APP Execution report received ORIGCLOID: ", oidbuf);
#endif

    capk::OrdStatus_t ordStatus = order.getOrdStatus();
/*
    uuidbuf_t testbuf;
    order_id_t ooo;
    pan::log_DEBUG("IM ABOUT TO BLOW SOME SHIT UP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    ooo.set(er.cl_order_id().c_str(), er.cl_order_id().size());
    pan::log_DEBUG("UUID as string: ", ooo.c_str(testbuf));
    pan::log_DEBUG("UUID_LEN: ", pan::integer(UUID_LEN));
    pan::log_DEBUG("CLOID: ", er.cl_order_id(), pan::integer(er.cl_order_id().size()));
    pan::log_DEBUG("ORIGCLOID: ", er.orig_cl_order_id(), pan::integer(er.orig_cl_order_id().size()));
*/

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
        clock_gettime(CLOCK_REALTIME, &ts); 
        pan::log_INFORMATIONAL("PARTIAL FILL->",
                        pan::blob(oid.get_uuid(), oid.size()), 
                        ",", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        ",", 
                        order.getExecId(), 
                        ",", 
                        pan::real(order.getLastShares()), 
                        ",", 
                        pan::real(order.getLastPrice()), 
                        ",", 
                        pan::real(order.getAvgPrice()), 
                        ",", 
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));
    }

    if (ordStatus == capk::ORD_STATUS_FILL) {
       if (order.getExecType() == capk::EXEC_TYPE_REPLACE) {
           pan::log_NOTICE("OID: ", pan::blob(oid.get_uuid(), oid.size()),
                   " replaced AND fully filled");
       }
       pan::log_INFORMATIONAL("FILL->",
                        pan::blob(oid.get_uuid(), oid.size()), 
                        ",", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        ",", 
                        order.getExecId(), 
                        ",", 
                        pan::real(order.getLastShares()), 
                        ",", 
                        pan::real(order.getLastPrice()), 
                        ",", 
                        pan::real(order.getAvgPrice()), 
                        ",", 
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));

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

        clock_gettime(CLOCK_REALTIME, &ts); 
        pan::log_DEBUG("REPLACE", 
                        "ORIGOID: ", 
                        pan::blob(oid.get_uuid(), oid.size()), 
                        "OID: ", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        " ",
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));

 
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
        clock_gettime(CLOCK_REALTIME, &ts); 
        pan::log_DEBUG("OID: ", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        " PENDING CANCEL (REALTIME) ", 
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));
            //(*((m.insert(value_type(k, data_type()))).first)).second
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

        clock_gettime(CLOCK_REALTIME, &ts); 
        pan::log_DEBUG("OID: ", 
                        pan::blob(origOid.get_uuid(), origOid.size()), 
                        " PENDING REPLACE (REALTIME) ", 
                        pan::integer(ts.tv_sec), 
                        ":", 
                        pan::integer(ts.tv_nsec));
 
    }

    if (ordStatus == capk::ORD_STATUS_REJECTED) {
        pan::log_WARNING("REJECTED!!!! OID: ", pan::blob(oid.get_uuid(), oid.size()),
                "ORIG OID: ", pan::blob(origOid.get_uuid(), origOid.size()));
        order_map_iter_t orderIter = pendingOrders.find(origOid);
        if (orderIter != pendingOrders.end()) {
            pan::log_DEBUG("Deleting rejected order from pending");
            pendingOrders.erase(orderIter);
        }
        else {
            pan::log_DEBUG("Rejected order not found in pending");
        }
    }

    //pan::log_DEBUG("Num pending orders: ", pan::integer(pendingOrders.size()));
    //pan::log_DEBUG("Num working orders: ", pan::integer(workingOrders.size()));
    //pan::log_DEBUG("Num completed orders: ", pan::integer(completedOrders.size()));

}

void
handleOrderCancelReject(capkproto::order_cancel_reject& ocr) 
{
#ifdef LOG
    pan::log_DEBUG("handleOrderCancelReject()"); 
#endif
    order_id_t oid;
    oid.set(ocr.orig_cl_order_id().c_str(), ocr.orig_cl_order_id().size());
#ifdef LOG
    uuidbuf_t oidbuf;
    oid.c_str(oidbuf);
    pan::log_WARNING("OID: ", oidbuf, 
            " cancel rejected - full msg follows\n", 
            ocr.DebugString());
#endif
/*
    order_map_iter_t pendingIter = pendingOrders.find(oid);
    if (orderIter == pendingOrders.end()) {
        pan::log_DEBUG("OID: ", 
                pan::blob(oid.get_uuid(), oid.size()), 
                " not found in pending orders");
    }
    else {
        pendingOrders.erase(orderIter);
    }

    order_map_iter_t workingIter = workingOrders.find(oid);
    if (orderIter == workingOrders.end()) {
        pan::log_DEBUG("OID: ", 
                pan::blob(oid.get_uuid(), oid.size()), 
                " not found in pending orders");
    }
    else {
        workingOrders.erase(orderIter);
    }
*/
#ifdef LOG
    pan::log_DEBUG("Num pending orders: ", pan::integer(pendingOrders.size()));
    pan::log_DEBUG("Num working orders: ", pan::integer(workingOrders.size()));
    pan::log_DEBUG("Num completed orders: ", pan::integer(completedOrders.size()));
#endif
}

bool
receiveOrder(zmq::socket_t* sock) 
{
    zmq::message_t msgtypeframe;
    zmq::message_t msgframe;
    bool rc;
    rc = sock->recv(&msgtypeframe, ZMQ_RCVMORE);
    if (*(static_cast<capk::msg_t*>(msgtypeframe.data())) == capk::EXEC_RPT) {
        bool parseOK;
        //pan::log_DEBUG("APP Received msg type: ", pan::integer(capk::EXEC_RPT), " - capk::EXEC_RPT");
        rc = sock->recv(&msgframe, 0);
        assert(rc);
        capkproto::execution_report er;
        parseOK = er.ParseFromArray(msgframe.data(), msgframe.size());
        assert(parseOK);
        //pan::log_DEBUG(er.DebugString());
        handleExecutionReport(er); 
        return true;
    }
    else if (*(static_cast<capk::msg_t*>(msgtypeframe.data())) == capk::ORDER_CANCEL_REJ) {
        bool parseOK;
        //pan::log_DEBUG("APP Received msg type: ", pan::integer(capk::ORDER_CANCEL_REJ), " - capk::ORDER_CANCEL_REJ");
        rc = sock->recv(&msgframe, 0);
        assert(rc);
        capkproto::order_cancel_reject ocr;
        parseOK = ocr.ParseFromArray(msgframe.data(), msgframe.size());
        assert(parseOK);
        //pan::log_DEBUG(ocr.DebugString());
        handleOrderCancelReject(ocr);
        return true;
    }
    else {
        pan::log_WARNING("APP received unknown msg type: ", 
               "msgtypeframe: ", 
               pan::blob(msgtypeframe.data(), msgtypeframe.size()), 
               "(", pan::integer(*(static_cast<int*>(msgtypeframe.data()))), ")", "\n",
               "msgframe: ",
               pan::blob(msgframe.data(), msgframe.size())); 
        return false;
    }
    return false;
}

void
printOrderHash(order_map_t& om)
{
    order_map_iter_t iter = om.begin();
    int i = 0;
    std::cout << "OID\t symbol\t side\t qty\t orig price\t avg price" << std::endl;
    for (iter = om.begin(); iter != om.end(); iter++, i++) {
        capk::Order o = iter->second;
        order_id_t key = iter->first;
        //char oidbuf[UUID_STRLEN];
        uuidbuf_t oidbuf;
        //char keybuf[UUID_STRLEN];
        uuidbuf_t keybuf;
        o.getOid().c_str(oidbuf);
        key.c_str(keybuf);
        std::cout << i 
            << ") " 
            << " [" << keybuf << "] " 
            << oidbuf << "\t" 
            << o.getSymbol() << "\t" 
            << (o.getSide() == capk::BID ? "B" : "S") 
            << "\t" << o.getOrdQty() 
            << "(" << o.getLeavesQty() << ")\t" 
            << o.getPrice() 
            << "\t" << o.getAvgPrice() 
            << std::endl;

        std::cout << o.getSymbol() << std::endl;
            
    }
}

void
list_orders()
{
    std::cout << "Working orders   (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderHash(workingOrders);
    std::cout << "Pending orders   (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderHash(pendingOrders);
    std::cout << "Completed orders (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderHash(completedOrders);
}

}; // namespace capk



