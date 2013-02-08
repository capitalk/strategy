#include <zmq.hpp>
#include <signal.h>
#include <unistd.h>

#include "google/dense_hash_map"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>
#include <boost/make_shared.hpp>

#include <uuid/uuid.h>

#include "proto/new_order_single.pb.h"
#include "proto/capk_globals.pb.h"
#include "proto/execution_report.pb.h"
#include "proto/order_cancel.pb.h"
#include "proto/order_cancel_reject.pb.h"
#include "proto/order_cancel_replace.pb.h"
#include "proto/spot_fx_md_1.pb.h"
#include "proto/venue_configuration.pb.h"

#include "strategy_base/client_order_interface.h"
#include "strategy_base/client_market_data_interface.h"
#include "strategy_base/order_mux.h"
#include "strategy_base/market_data_mux.h"
#include "strategy_base/order.h"
#include "strategy_base/strategy_protocol.h"
#include "strategy_base/order_manager.h"

#include "utils/logging.h"
#include "utils/timing.h"
#include "utils/time_utils.h"
#include "utils/msg_types.h"
#include "utils/bbo_book_types.h"
#include "utils/venue_globals.h"
#include "utils/constants.h"
#include "utils/order_constants.h"
#include "utils/types.h"
#include "utils/config_server.h"

#define USE_MARKET_DATA 1

// namespace stuff
using google::dense_hash_map;
namespace po = boost::program_options;
po::options_description desc("Allowed options");

// Global vars
const char* const STRATEGY_ID =  "7b257b19-cd7a-4c97-b16c-98c70aff3d86";
const char* const CONFIG_SERVER_ADDR =  "tcp://127.0.0.1:11111";
capk::strategy_id_t sid;

const char* const ORDER_MUX = "inproc://order_mux";
const char* const MD_MUX = "inproc://md_mux";

#define MAX_MSGSIZE 256

// Global zmq context
zmq::context_t ctx(1);

// Global sockets these are PAIRS and the only two endpoints into the strategy
// order entry socket
zmq::socket_t* pOEInterface = NULL;
// market data socket
zmq::socket_t* pMDInterface = NULL;

// Single order manager - never create MORE THAN ONE!!! 
// And no - I dont' want to make this a singleton - I'm inclinded to agree with
// http://jalf.dk/blog/2010/03/singletons-solving-problems-you-didnt-know-you-never-had-since-1995/
capk::OrderManager *om;

// Order multiplexer and its thread
capk::OrderMux* ptr_order_mux = NULL;
boost::thread* omux_thread = NULL;

// Market data multiplexer and its thread
capk::MarketDataMux* ptr_market_data_mux = NULL;
boost::thread* mdmux_thread = NULL;

// Multi market book for a single instrument
// Valid whether we are using aggregated or straight feed
capk::InstrumentBBO_t bbo_book;

// Configuration settings returned from config_server
capkproto::configuration all_venue_config;

// Signal handler setup for ZMQ
static int s_interrupted = 0;

// Order map
capk::SharedOrderMap_t som;

// Timing variables
timespec t0;
bool t0_stamped = false;
timespec t1;
timespec t2;
timespec t3;
timespec t4;
timespec t5;
timespec t6;
timespec t7;
timespec t8;
timespec t9;

static void s_signal_handler (int signal_value)
{
    s_interrupted = 1;
}

static void s_catch_signals (void)
{
    struct sigaction action;
    action.sa_handler = s_signal_handler;
    action.sa_flags = 0;
    sigemptyset (&action.sa_mask);
    sigaction (SIGINT, &action, NULL);
    sigaction (SIGTERM, &action, NULL);
}

void
deleteOrder(capk::Order_ptr_t op)
{
    som.erase(op->getClOid());
}

void
printMap(capk::SharedOrderMap_t &som)
{
    capk::SharedOrderMapIter_t it = som.begin();
    while (it != som.end()) {
        std::cout << "ORDER:" << *(it->second) << std::endl;
        it++;
    }
}

int
main(int argc, char **argv)
{
    capk::order_id_t oid1(true);
    capk::uuidbuf_t oid1buf;
    oid1.c_str(oid1buf);
    pan::log_DEBUG("OID1:<", oid1buf, ">");
    capk::order_id_t oid2(true);
    oid2 = oid1;
    capk::uuidbuf_t oid2buf;
    oid2.c_str(oid2buf);
    pan::log_DEBUG("OID2:<", oid2buf, ">");


    capk::Order o1;
    std::cerr << "Order object raw: " << o1 << std::endl;
    capkproto::new_order_single nos;
    nos.set_cl_order_id(oid1.get_uuid(), oid1.size());
    o1.set(nos);
    std::cerr << "Order after nos assign: " << o1 << std::endl;

    capk::Order o2(o1);

    capk::Order_ptr_t optr;
    optr = boost::make_shared<capk::Order>(o1);
    std::cerr << "Pointer use count is: " << optr.use_count() << std::endl;
    capk::order_id_t oid_empty("");
    capk::order_id_t oid_deleted("1");
    som.set_empty_key(oid_empty);
    som.set_deleted_key(oid_deleted);
    capk::SharedOrderMapInsert_t ins = som.insert(capk::SharedOrderMapValue_t(oid1, optr));
    std::cout << "MAP:\n";
    printMap(som);

    std::cerr << "Pointer use count is: " << optr.use_count() << std::endl;

    //capk::Order_ptr_t external_optr = optr;
    std::cerr << "Pointer use count is: " << optr.use_count() << std::endl;
    std::cerr << "Deleting oid1" << std::endl;
    deleteOrder(optr);
    std::cerr << "Pointer use count is: " << optr.use_count() << std::endl;
    capk::SharedOrderMapIter_t found = som.find(oid1);
    if (found == som.end()) {
        std::cerr << "ORDER NOT FOUND IN MAP!" << std::endl;
    }
    else {
        std::cerr << "FOUND ORDER IN MAP" << std::endl;
    }
    std::cout << "MAP:\n";
    std::cerr << "Pointer use count is: " << optr.use_count() << std::endl;
    printMap(som);
    //assert (*(found->second) == o1);

    return 0;

}




