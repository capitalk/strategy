#ifndef __ORDER_MANAGER__
#define __ORDER_MANAGER__


#include <zmq.hpp>
#include <signal.h>


#include "google/dense_hash_map"

#include "proto/new_order_single.pb.h"
#include "proto/capk_globals.pb.h"
#include "proto/execution_report.pb.h"
#include "proto/order_cancel.pb.h"
#include "proto/order_cancel_reject.pb.h"
#include "proto/order_cancel_replace.pb.h"
#include "proto/spot_fx_md_1.pb.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>

#include <cstdatomic>

#include <uuid/uuid.h>

#include "client_order_interface.h"
#include "client_market_data_interface.h"
#include "order_mux.h"
#include "market_data_mux.h"
#include "order.h"

#include "utils/msg_types.h"
#include "utils/order_constants.h"
#include "utils/logging.h"
#include "utils/timing.h"
#include "utils/time_utils.h"
#include "utils/bbo_book_types.h"
#include "utils/types.h"
#include "utils/venue_globals.h"

#include "strategy_base/strategy_protocol.h"

typedef void (*handler_t)(const void*);

// namespace stuff
using google::dense_hash_map;

namespace capk {

// Hash tables and typedefs for storing order states
//typedef dense_hash_map<order_id_t, Order, std::tr1::hash<order_id>, eq_order_id> OrderMap_t;
//typedef OrderMap_t::iterator OrderMapIter_t;
//typedef std::pair<OrderMapIter_t, bool> OrderMapInsert_t;
//typedef std::pair<order_id_t, Order> OrderMapValue_t;
// Testing using shared_ptrs for storage rather than copying a lot of shit in and out
// of the hashtable
typedef dense_hash_map<order_id_t, Order_ptr_t, std::tr1::hash<order_id>, eq_order_id> SharedOrderMap_t;
typedef SharedOrderMap_t::iterator SharedOrderMapIter_t;
typedef std::pair<SharedOrderMapIter_t, bool> SharedOrderMapInsert_t;
typedef std::pair<order_id_t, Order_ptr_t> SharedOrderMapValue_t;

class OrderManager 
{
    public:
    OrderManager(zmq::socket_t* oi,
            const strategy_id_t& sid);
    ~OrderManager();

    void handleExecutionReport(capkproto::execution_report& er);

    void handleOrderCancelReject(capkproto::order_cancel_reject& ocr);

    void handle_fill(const Order&); 

    bool get_order(const order_id_t& cl_order_id, capk::Order& o);

    bool receiveOrder(zmq::socket_t* sock);

    void printOrderMap(SharedOrderMap_t& om);

    void list_orders();


    order_id_t send_new_order(const venue_id_t& venue_id,
        const char* symbol,
        const capk::Side_t& side,
        const double price,
        const double qty);

    order_id_t send_cancel(const capk::venue_id_t& venue_id,
        const capk::order_id_t& orig_order_id,
        const char* symbol,
        const capk::Side_t side,
        const double qty);

    order_id_t send_cancel_replace(const capk::venue_id_t& venue_id,
        const capk::order_id_t& orig_order_id,
        const char* symbol,
        const capk::Side_t side,
        const double new_price,
        const double new_qty);

    void set_callback_order_new(handler_t callback) {
        _callback_order_new = callback;
    }

    void set_callback_order_fill(handler_t callback) {
        _callback_order_fill = callback;
    }

    void set_callback_order_reject(handler_t callback) {
        _callback_order_reject = callback;
    }

#ifdef DEBUG
    void DBG_ORDER_MAP();
#endif

    private:
    // KTK - TODO do we really need to keep three MAPs? 
    // Can pending, working, and completed just be lists
    // or orders? Is this faster? Or is having the shared_ptr
    // to the order in the map better? I think latter for now
    // due to locality of ref but should be tested.
    SharedOrderMap_t _pending_orders;
    SharedOrderMap_t _working_orders;
    SharedOrderMap_t _completed_orders;	
    SharedOrderMap_t _all_orders;	

    zmq::socket_t* _order_interface;
    strategy_id_t _sid;

    OrderManager& operator=(const OrderManager& om);
    OrderManager(const OrderManager& om);
    OrderManager();

    void get_new_order_id(order_id_t* oid);
    void generate_order_id_cache(order_id_t** oid);
    std::atomic<int> _order_id_cache_idx;
    int _order_id_cache_size;
    order_id_t* _order_id_cache;

    handler_t _callback_order_fill;
    handler_t _callback_order_new;
    handler_t _callback_order_reject;

    // Internal handler functions
    void _handle_exec_new(capk::Order_ptr_t &order);
    void _handle_exec_cancel(capk::Order_ptr_t &order);
    void _handle_exec_reject(capk::Order_ptr_t &order);

};

} // namespace capk

#endif // __ORDER_MANAGER__
