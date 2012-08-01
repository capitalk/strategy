#ifndef __STRATEGY_PROTOCOL__
#define __STRATEGY_PROTOCOL__

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

#define MAX_MSGSIZE 256

int snd_HELO(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id,
        const capk::venue_id_t venueID);

int 
snd_HELO(zmq::socket_t* direct_order_interface, 
        const char* interface_addr, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venue_id);

int
rcv_HELO(zmq::socket_t* direct_order_interface, 
        const capk::venue_id_t& venue_id);

void snd_ORDER_CANCEL_REPLACE(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id,
        const capk::venue_id_t venueID, 
        capkproto::order_cancel_replace& ocr);

void snd_ORDER_CANCEL(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id,
        const capk::venue_id_t venueID, 
        capkproto::order_cancel& oc);

void snd_NEW_ORDER(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id,
        const capk::venue_id_t venueID, 
        capkproto::new_order_single& nos);


#endif // __STRATEGY_PROTOCOL__
