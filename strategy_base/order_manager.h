#ifndef __ORDER_MANAGER__
#define __ORDER_MANAGER__

#include "logging.h"
#include "timing.h"

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

#include <uuid/uuid.h>

#include "msg_cache.h"
#include "msg_types.h"
#include "strategy_base/client_order_interface.h"
#include "strategy_base/client_market_data_interface.h"
#include "strategy_base/order_mux.h"
#include "strategy_base/market_data_mux.h"
#include "strategy_base/order.h"

#include "utils/time_utils.h"
#include "utils/bbo_book_types.h"
#include "utils/types.h"
#include "utils/venue_globals.h"

// namespace stuff
using google::dense_hash_map;



// Hash tables and typedefs for storing order states
typedef dense_hash_map<order_id_t, capk::Order, std::tr1::hash<order_id>, eq_order_id> order_map_t;
typedef order_map_t::iterator order_map_iter_t;
typedef std::pair<order_map_iter_t, bool> order_map_insert_t;
typedef std::pair<order_id_t, capk::Order> order_map_value_t;
/*
order_map_t pendingOrders;
order_map_t workingOrders;
order_map_t completedOrders;	
*/
void list_orders();

capkproto::new_order_single
create_order(const strategy_id_t& sid,
                const char* symbol, 
				capk::Side_t side,
				double quantity,
				double price);


void handleExecutionReport(capkproto::execution_report& er);

void handleOrderCancelReject(capkproto::order_cancel_reject& ocr);

bool receiveOrder(zmq::socket_t* sock);

void printOrderHash(order_map_t& om);

void list_orders();


#endif // __ORDER_MANAGER__
