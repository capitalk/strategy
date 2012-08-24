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

// namespace stuff
using google::dense_hash_map;

namespace capk {

// Hash tables and typedefs for storing order states
typedef dense_hash_map<order_id_t, Order, std::tr1::hash<order_id>, eq_order_id> order_map_t;
typedef order_map_t::iterator order_map_iter_t;
typedef std::pair<order_map_iter_t, bool> order_map_insert_t;
typedef std::pair<order_id_t, Order> order_map_value_t;

extern order_map_t pendingOrders;
extern order_map_t workingOrders;
extern order_map_t completedOrders;	

void list_orders();

//capkproto::new_order_single
void
create_order(capkproto::new_order_single* nos, 
                const strategy_id_t& sid,
                const char* symbol, 
				capk::Side_t side,
				double quantity,
				double price);


void handleExecutionReport(capkproto::execution_report& er);

void handleOrderCancelReject(capkproto::order_cancel_reject& ocr);

bool receiveOrder(zmq::socket_t* sock);

void printOrderHash(order_map_t& om);

void list_orders();

} // namespace capk

#endif // __ORDER_MANAGER__
