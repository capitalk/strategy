#include "order_manager.h"


#include <zmq.hpp>
#include <signal.h>


#include "google/dense_hash_map"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>
#include <boost/make_shared.hpp>

#include <uuid/uuid.h>

#include "utils/msg_types.h"
#include "utils/order_constants.h"
#include "utils/logging.h"
#include "utils/timing.h"
#include "utils/time_utils.h"
#include "utils/bbo_book_types.h"
#include "utils/types.h"
#include "utils/venue_globals.h"

#include "strategy_base/strategy_protocol.h"

using google::dense_hash_map;

namespace capk {


OrderManager::OrderManager(zmq::socket_t* oi, const strategy_id_t& sid):
    _order_interface(oi), 
    _sid(sid),
    _order_id_cache_idx(0),
    _order_id_cache_size(100000),
    _order_id_cache(NULL),
    _callback_order_fill(NULL),
    _callback_order_new(NULL),
    _callback_order_reject(NULL)
{
    // Set empty keys
    capk::order_id_t oidEmpty("");
    //_pending_orders.set_empty_key(oidEmpty);
    //_working_orders.set_empty_key(oidEmpty);
    //_completed_orders.set_empty_key(oidEmpty);
    _all_orders.set_empty_key(oidEmpty);

    // Set deleted keys
    order_id_t oidDeleted("1");
    //_pending_orders.set_deleted_key(oidDeleted);
    //_working_orders.set_deleted_key(oidDeleted);
    //_completed_orders.set_deleted_key(oidDeleted);
    _all_orders.set_deleted_key(oidDeleted);

    // Generate order id cache
    generate_order_id_cache(&_order_id_cache);
}

OrderManager::~OrderManager()
{
    delete[] _order_id_cache;
}

void
OrderManager::generate_order_id_cache(order_id_t** oid_cache)
{
    if (!oid_cache) {
        return;
    }
    if (*oid_cache != NULL) {
        pan::log_DEBUG("Deleting and regenerating order_id_cache");
        delete [] oid_cache;    
    }
    (*oid_cache) = new order_id_t[_order_id_cache_size];
    for (int i=0; i<_order_id_cache_size; i++) {
        (*oid_cache)[i] = order_id_t(true);
    }
}

void
OrderManager::get_new_order_id(order_id_t* oid) 
{
    if (oid == NULL) {
        return;
    }
    _order_id_cache_idx.fetch_add(1, std::memory_order_consume); 
    if (_order_id_cache_idx >= _order_id_cache_size) {
        generate_order_id_cache(&_order_id_cache);
    }
    *oid = _order_id_cache[_order_id_cache_idx];
}


void
OrderManager::handle_fill(const Order &order)
{
#ifdef LOG
    pan::log_DEBUG("handle_fill - ", order.getSymbol()," ", 
            pan::character((char)order.getSide()), " ", 
            pan::real(order.getOrdQty()),"/", 
            pan::real(order.getCumQty()), " ", 
            "execType:", pan::character((char)order.getExecType()), " ",
            "ordStatus:", pan::character((char)order.getOrdStatus()));
#endif
    if (_callback_order_fill) {
        (*_callback_order_fill)(static_cast<const void*>(&order));
    }
}


void
OrderManager::_handle_exec_new(capk::Order_ptr_t &order)
{
    order->set_state(OrderState_t::INT_STATE_NEW);
    if (_callback_order_new) {
        (*_callback_order_new)(static_cast<const void*>(order.get()));
    }
}

void
OrderManager::_handle_exec_cancel(capk::Order_ptr_t &order)
{
    order->set_state(OrderState_t::INT_STATE_CANCELED);
    uuidbuf_t oidbuf;
    order->getClOid().c_str(oidbuf);
    pan::log_DEBUG("CANCELLED: Removing: ", oidbuf, " from orders");
    // Remove the original order
    _all_orders.erase(order->getOrigClOid());
    // Remove the cancel request
    _all_orders.erase(order->getClOid());
}

void
OrderManager::_handle_exec_reject(capk::Order_ptr_t &order)
{
    order->set_state(OrderState_t::INT_STATE_REJECTED);
    if (_callback_order_reject) {
        (*_callback_order_reject)(static_cast<const void*>(order.get()));
    }
    uuidbuf_t oidbuf;
    order->getClOid().c_str(oidbuf);
    pan::log_DEBUG("REJECTED: Removing: ", oidbuf, " from orders");
    _all_orders.erase(order->getClOid());
}

void 
OrderManager::handleExecutionReport(capkproto::execution_report& er) 
{
    timespec ts;
    capk::Order order;
    order.set(const_cast<capkproto::execution_report&>(er));
    order_id_t cl_order_id = order.getClOid();

#ifdef LOG
    uuidbuf_t oidbuf;
    cl_order_id.c_str(oidbuf);
    pan::log_DEBUG("handle_execution_report received CLOID: ", oidbuf);
#endif

    order_id_t orig_cl_order_id = order.getOrigClOid();
    if (orig_cl_order_id.is_empty()) {
        orig_cl_order_id = cl_order_id;
    }

#ifdef LOG
    orig_cl_order_id.c_str(oidbuf);
    pan::log_DEBUG("handle_execution_report received ORIGCLOID: ", oidbuf);
#endif

    capk::OrdStatus_t ordStatus = order.getOrdStatus();
    capk::ExecType_t execType  = order.getExecType();
    capk::ExecTransType_t execTransType  = order.getExecTransType();
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

#ifdef DEBUG
    pan::log_DEBUG("Full execution report protobuf:\n", er.DebugString());
#endif

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

    // Check for unsupported transaction types
    if (execTransType == capk::EXEC_TRAN_CANCEL || 
            execTransType == capk::EXEC_TRAN_CORRECT || 
            execTransType == capk::EXEC_TRAN_STATUS) {
        pan::log_ALERT("UNSUPPORTED EXEC TRANS TYPE(20):", pan::character((char)execTransType));
    }

    if (execType == capk::EXEC_TYPE_STOPPED || 
            execType == capk::EXEC_TYPE_SUSPENDED ||
            execType == capk::EXEC_TYPE_RESTATED || 
            execType == capk::EXEC_TYPE_CALCULATED) {
        pan::log_ALERT("UNSUPPORTED EXEC TYPE(150):", pan::character((char)execType));
    }
    
    
    SharedOrderMapIter_t currentOrderIter = _all_orders.find(cl_order_id);
    assert(currentOrderIter != _all_orders.end());

    if (currentOrderIter == _all_orders.end()) {
        pan::log_CRITICAL("Received unknown cl_order_id:", 
                pan::blob(currentOrderIter->second->getClOid().get_uuid(), 
                    currentOrderIter->second->getClOid().size()));
    }
    capk::Order_ptr_t currentOrder = currentOrderIter->second;
    currentOrder->update(order);    
    
    if (execTransType == capk::EXEC_TRAN_NEW) {
        if (execType == capk::EXEC_TYPE_NEW) { 
            pan::log_DEBUG("NEW ORDER");
            _handle_exec_new(currentOrder);
        }
        else if (execType == capk::EXEC_TYPE_CANCELLED) {
            pan::log_DEBUG("CANCEL");
            _handle_exec_cancel(currentOrder);
        }
        else if (execType == capk::EXEC_TYPE_FILL) {
            pan::log_DEBUG("Received fill");
            if (ordStatus == capk::ORD_STATUS_FILL) {
                pan::log_DEBUG("-- FULL FILL");
                currentOrder->set_state(OrderState_t::INT_STATE_FILLED);
            }
            else if (ordStatus == capk::ORD_STATUS_PARTIAL_FILL) {
                pan::log_DEBUG("-- PARTIAL FILL");
                currentOrder->set_state(OrderState_t::INT_STATE_PARTIALLY_FILLED);
            }
            handle_fill(order);
        }
        else if (execType == capk::EXEC_TYPE_PARTIAL_FILL) {
            currentOrder->set_state(OrderState_t::INT_STATE_PARTIALLY_FILLED);
            pan::log_DEBUG("-- PARTIAL FILL");
            handle_fill(order);
        }
        else if (execType == capk::EXEC_TYPE_REJECTED) {
            pan::log_DEBUG("REJECTED ");
            _handle_exec_reject(currentOrder);
        }
        else if (execType == capk::EXEC_TYPE_PENDING_CANCEL) {
            pan::log_DEBUG("Received pending cancel");
            currentOrder->set_state(OrderState_t::INT_STATE_PENDING_CANCEL);
        }
        else if (execType == capk::EXEC_TYPE_REPLACE) {
            currentOrder->set_state(OrderState_t::INT_STATE_REPLACED);
            pan::log_DEBUG("REPLACE");
            currentOrder->setClOid(cl_order_id);
        }
        else if (execType == capk::EXEC_TYPE_SUSPENDED) {
            uuidbuf_t oidbuf;
            cl_order_id.c_str(oidbuf);
            uuidbuf_t origoidbuf;
            orig_cl_order_id.c_str(origoidbuf);
            currentOrder->set_state(OrderState_t::INT_STATE_SUSPENDED);
            pan::log_CRITICAL("Trade SUSPENDED - oid:", 
                    oidbuf, 
                    " orig_cl_ord_id: ", 
                    origoidbuf,
                    " wait for promotion/demotion for trade");
        }
    }
    else if (execTransType == capk::EXEC_TRAN_CANCEL) {
        uuidbuf_t oidbuf;
        cl_order_id.c_str(oidbuf);
        uuidbuf_t origoidbuf;
        orig_cl_order_id.c_str(origoidbuf);
        pan::log_ALERT("Unsolicited CANCEL (busted exec) request for oid: ", 
                oidbuf, 
                " orig_cl_ord_id: ", 
                origoidbuf);
        pan::log_ALERT("New exec:", "price:", pan::real(order.getPrice()), 
                "qty:", pan::real(order.getOrdQty()), 
                "cum_qty:", pan::real(order.getCumQty()), 
                "leaves_qty:", pan::real(order.getLeavesQty()), 
                "avg_price:", pan::real(order.getAvgPrice()), 
                "order_status:", pan::character((char)order.getOrdStatus()));
        pan::log_ALERT("Incoming er protobuf:\n", er.DebugString());
    }
    else if (execTransType == capk::EXEC_TRAN_CORRECT) {
        uuidbuf_t oidbuf;
        cl_order_id.c_str(oidbuf);
        uuidbuf_t origoidbuf;
        orig_cl_order_id.c_str(origoidbuf);
        pan::log_ALERT("Unsolicited CORRECT (busted exec) request for oid: ", oidbuf, " orig_cl_ord_id: ", origoidbuf);
        pan::log_ALERT("Corrected exec:", "price:", pan::real(order.getPrice()), 
                "qty:", pan::real(order.getOrdQty()), 
                "cum_qty:", pan::real(order.getCumQty()), 
                "leaves_qty:", pan::real(order.getLeavesQty()), 
                "avg_price:", pan::real(order.getAvgPrice()), 
                "order_status:", pan::character((char)order.getOrdStatus()));
        pan::log_ALERT("Incoming er protobuf:\n", er.DebugString());
    }

    // KTK TODO - make function is_terminal_state() return 1/0 
    if (ordStatus == capk::ORD_STATUS_FILL ||
            ordStatus == capk::ORD_STATUS_CANCELLED ||
            ordStatus == capk::ORD_STATUS_REJECTED || 
            ordStatus == capk::ORD_STATUS_EXPIRED) {
        uuidbuf_t oidbuf;
        cl_order_id.c_str(oidbuf);
        pan::log_DEBUG("Removing: ", oidbuf, " from orders");
        _all_orders.erase(cl_order_id);
    }
}

void
OrderManager::handleOrderCancelReject(capkproto::order_cancel_reject& ocr) 
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
#ifdef LOG
    pan::log_DEBUG("ORDER COUNT (all): ", pan::integer(_all_orders.size()));
#endif
}

bool
OrderManager::receiveOrder(zmq::socket_t* sock) 
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


/*
 * Must check for is_empty on return
 */
order_id_t
OrderManager::send_new_order(const venue_id_t& venue_id, 
        const char* symbol,
        const capk::Side_t& side,
        const double price,
        const double qty)
{

    if (this->_order_interface != NULL) {
        // create order protobuf and fill it in
        capkproto::new_order_single nos;
        nos.set_strategy_id(this->_sid.get_uuid(), this->_sid.size());
        nos.set_symbol(symbol);
        if (side == capk::BID) {
            nos.set_side(capkproto::BID);
        }
        else if (side == capk::ASK) {
            nos.set_side(capkproto::ASK);
        }
        nos.set_price(price);
        nos.set_order_qty(qty);
        nos.set_ord_type(capkproto::LIM);
        nos.set_time_in_force(capkproto::GFD);

        order_id_t cl_order_id;
        this->get_new_order_id(&cl_order_id);
        nos.set_cl_order_id(cl_order_id.get_uuid(), cl_order_id.size());
/*
        assert(!order_id.is_empty());
        nos.set_order_id(order_id.get_uuid(), order_id.size());
*/
        // send the order to the order interface
        capk::snd_NEW_ORDER(this->_order_interface, 
                this->_sid, 
                cl_order_id,
                venue_id, 
                nos);

        capk::Order_ptr_t order_ptr = boost::make_shared<Order>(cl_order_id);
        order_ptr->set(nos);
        order_ptr->set_state(OrderState_t::INT_STATE_PENDING_NEW);
        SharedOrderMapValue_t new_map_pair(cl_order_id, order_ptr);
        SharedOrderMapInsert_t newInsert = 
            _all_orders.insert(new_map_pair);
        assert(newInsert.second);
        if (newInsert.second != true) {
            pan::log_ALERT("Adding new order to all_orders failed!");
        }

        return cl_order_id;
    }
    else {
        pan::log_CRITICAL("send_new_order - order interface is NULL");
        capk::order_id_t empty_order_id;
        return empty_order_id;
        // must check for is_empty on return
    }
}


// KTK - TODO - qty is not required here - remove it
order_id_t
OrderManager::send_cancel(const capk::venue_id_t& venue_id, 
        const capk::order_id_t& orig_cl_order_id,
        const char* symbol, 
        const capk::Side_t side,
        const double qty)
{
    if (this->_order_interface != NULL) {
        capkproto::order_cancel oc;
        oc.set_strategy_id(this->_sid.get_uuid(), this->_sid.size());  
        oc.set_symbol(symbol);
        if (side == capk::BID) {
            oc.set_side(capkproto::BID);
        }
        else if (side == capk::ASK) {
            oc.set_side(capkproto::ASK);
        }
        oc.set_order_qty(qty);

        order_id_t cl_order_id(false);
        this->get_new_order_id(&cl_order_id);
        oc.set_cl_order_id(cl_order_id.get_uuid(), cl_order_id.size());
        oc.set_orig_cl_order_id(orig_cl_order_id.get_uuid(), orig_cl_order_id.size());

        capk::snd_ORDER_CANCEL(this->_order_interface, 
                this->_sid, 
                orig_cl_order_id, 
                cl_order_id, 
                venue_id,
                oc);

        // Doesn't need to necessarily be live in working orders since 
        // we may send a cancel before the original order is acknowleged 
        // and has not been added to working orders
        // This also means that we may get a now such order and receive
        // a cancel reject if the order is not on the market so this
        // is really very non-deterministic. 
        /*
        SharedOrderMapIter_t workingOrder = _working_orders.find(orig_cl_order_id);
        if (workingOrder == _working_orders.end()) {
            pan::log_ALERT("send_cancel - orig_cl_order_id is not currently live");
        }
        */
        capk::Order_ptr_t order_ptr = boost::make_shared<Order>(cl_order_id);
        // No need to set the order to contain the entire protobuf since we're just
        // using the entry of this order to keep track of the fact that we sent the 
        // cancel request
        //order_ptr->set(nos);
        order_ptr->set_state(OrderState_t::INT_STATE_PENDING_CANCEL);
        SharedOrderMapValue_t new_map_pair(cl_order_id, order_ptr);
        SharedOrderMapInsert_t newInsert = 
            _all_orders.insert(new_map_pair);
        assert(newInsert.second);
        if (newInsert.second != true) {
            pan::log_ALERT("Adding new order to all_orders failed!");
        }

        SharedOrderMapIter_t cancelOrder = _all_orders.find(orig_cl_order_id);
        assert(cancelOrder != _all_orders.end());
        if (cancelOrder == _all_orders.end()) {
            pan::log_ALERT("send_cancel - orig_cl_order_id not found in all orders");
        }
        cancelOrder->second->set_state(OrderState_t::INT_STATE_PENDING_CANCEL);

        return cl_order_id;
    }
    else {
        capk::order_id_t o;
        return o;
    }
}

order_id_t
OrderManager::send_cancel_replace(const capk::venue_id_t& venue_id,
        const capk::order_id_t& orig_cl_order_id, 
        const char* symbol,
        const capk::Side_t side,
        const double new_price, 
        const double new_qty)
{
    if (this->_order_interface != NULL) {
        capkproto::order_cancel_replace ocr;
        ocr.set_strategy_id(this->_sid.get_uuid(), this->_sid.size());
        ocr.set_symbol(symbol);
        if (side == capk::BID) {
            ocr.set_side(capkproto::BID);
        }
        else if (side == capk::ASK) {
            ocr.set_side(capkproto::ASK);
        }
        ocr.set_ord_type(capkproto::LIM);
        ocr.set_order_qty(new_qty);
        ocr.set_price(new_price);

        order_id_t cl_order_id;
        this->get_new_order_id(&cl_order_id);
        ocr.set_cl_order_id(cl_order_id.get_uuid(), cl_order_id.size());
        ocr.set_orig_cl_order_id(orig_cl_order_id.get_uuid(), orig_cl_order_id.size());
        
        capk::snd_ORDER_CANCEL_REPLACE(this->_order_interface, 
                this->_sid, 
                orig_cl_order_id, 
                cl_order_id, 
                venue_id, 
                ocr);

        SharedOrderMapIter_t cancelReplaceOrder = _all_orders.find(orig_cl_order_id);
        assert(cancelReplaceOrder != _all_orders.end());
        if (cancelReplaceOrder == _all_orders.end()) {
            pan::log_ALERT("send_cancel_replace - orig_cl_order_id not found in all orders",
                    pan::blob(orig_cl_order_id.get_uuid(), orig_cl_order_id.size()));
        }
        cancelReplaceOrder->second->set_state(OrderState_t::INT_STATE_PENDING_REPLACE);

        // KTK - NB in the optimistic scenario that we're supposed to operate under 
        // we should just change working order id in the working orders but if the cancel
        // or modification is rejected then we need to revert the order 
        // id to orig_cl_order_id
        // Turns out that this information is available when the order is rejected (150=8)
        // and the cl_order_id (11) and the orig_cl_order_id (41) are returned in the rejection message.
        // Note that some venues don't support cancel/replace and only support cancel in 
        // which case the semantics still hold.
        // FIX needs to be FIXED. 

        return cl_order_id;
    }
    else {
        capk::order_id_t o;
        return o;
    }
}

bool
OrderManager::get_order(const order_id_t& cl_order_id, capk::Order& o)
{
    pan::log_DEBUG("get_order received cl_order_id: ", pan::blob(cl_order_id.get_uuid(), cl_order_id.size()));
    SharedOrderMapIter_t order = _all_orders.find(cl_order_id);
    //assert(order != _all_orders.end());
    if (order != _all_orders.end()) {
        o = *(order->second);
        return true;
    }
    return false;

}

#ifdef DEBUG
void 
OrderManager::DBG_ORDER_MAP()
{
    printOrderMap(_pending_orders);
    printOrderMap(_working_orders);
    printOrderMap(_completed_orders);
    printOrderMap(_all_orders);
}
#endif

void
OrderManager::printOrderMap(SharedOrderMap_t& om)
{
    SharedOrderMapIter_t iter = om.begin();
    int i = 0;
    //std::cout << "OID\t symbol\t side\t qty\t orig price\t avg price" << std::endl;
    for (iter = om.begin(); iter != om.end(); iter++, i++) {
        capk::Order o = *iter->second;
        std::cout << o << std::endl;
        /*
        order_id_t key = iter->first;
        uuidbuf_t oidbuf;
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
        */
            
    }
}

void
OrderManager::list_orders()
{
    std::cout << "Working orders   (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderMap(_working_orders);
    std::cout << "Pending orders   (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderMap(_pending_orders);
    std::cout << "Completed orders (OID, symbol, side, qty(working), price) " << std::endl;
    printOrderMap(_completed_orders);
}

}; // namespace capk



