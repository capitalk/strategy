#include <zmq.hpp>
#include <signal.h>
#include <unistd.h>

#include "google/dense_hash_map"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>

#include <uuid/uuid.h>

#include "proto/new_order_single.pb.h"
#include "proto/capk_globals.pb.h"
#include "proto/execution_report.pb.h"
#include "proto/order_cancel.pb.h"
#include "proto/order_cancel_reject.pb.h"
#include "proto/order_cancel_replace.pb.h"
#include "proto/spot_fx_md_1.pb.h"
#include "proto/venue_configuration.pb.h"

#include "strategy_base-C++/client_order_interface.h"
#include "strategy_base-C++/client_market_data_interface.h"
#include "strategy_base-C++/order_mux.h"
#include "strategy_base-C++/market_data_mux.h"
#include "strategy_base-C++/order.h"
#include "strategy_base-C++/strategy_protocol.h"
#include "strategy_base-C++/order_manager.h"

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

bool
receiveVenueBBOMarketData(zmq::socket_t* sock, capk::InstrumentBBO_t* bbo_book)
{
    if (bbo_book == NULL) {
        return false;
    }
    capkproto::instrument_bbo venue_bbo_protobuf;
    zmq::message_t tick_msg;
    assert(sock);
    bool rc;
    rc = sock->recv(&tick_msg, ZMQ_NOBLOCK);
    assert(rc);
    venue_bbo_protobuf.ParseFromArray(tick_msg.data(), tick_msg.size());
#ifdef LOG
    //pan::log_INFORMATIONAL("receiveVenueBBOMarketData()");
    //pan::log_DEBUG("MD dump raw:", pan::blob(tick_msg.data(), tick_msg.size()), 
    //        "[", pan::integer(tick_msg.size()), "]");
    //pan::log_DEBUG("MD dump prb:", venue_bbo_protobuf.DebugString());
#endif

    // TODO FIX THIS to be int id for mic rather than string	
    // OK - 20120717
    if (venue_bbo_protobuf.symbol() == "EUR/USD") {
        if (!t0_stamped) {
            clock_gettime(CLOCK_MONOTONIC, &t0);
            t0_stamped = true;
        }

#ifdef LOG
        //pan::log_DEBUG("Received market data:\n", 
               //venue_bbo_protobuf.DebugString());
#endif

        bbo_book->bid_venue_id = venue_bbo_protobuf.bid_venue_id();
        bbo_book->bid_price = venue_bbo_protobuf.bid_price();
        bbo_book->bid_size = venue_bbo_protobuf.bid_size();
        //clock_gettime(CLOCK_MONOTONIC, &bbo_book.bid_last_update);

        bbo_book->ask_venue_id = venue_bbo_protobuf.ask_venue_id();
        bbo_book->ask_price = venue_bbo_protobuf.ask_price();
        bbo_book->ask_size = venue_bbo_protobuf.ask_size();
        //clock_gettime(CLOCK_MONOTONIC, &bbo_book.ask_last_update);
        return true;
    }
    return false;
}

void
print_timings()
{
    timespec delta1 = capk::timespec_delta(t0, t1);
    timespec delta2 = capk::timespec_delta(t1, t2);
    timespec delta3 = capk::timespec_delta(t2, t3);
    timespec delta4 = capk::timespec_delta(t4, t5);
    std::cerr << "Delta1 (tick to order_entry): " << delta1 << std::endl;
    std::cerr << "Delta2 (order_entry to new_order): " << delta2 << std::endl;
    std::cerr << "Delta3 (new_order to fill (from venue)): " << delta3 << std::endl;
    std::cerr << "Delta4 (new_order (rejected) to reject (from venue)): " << delta4 << std::endl;
}

void
order_new(const void* order)
{
    const capk::Order* o = (static_cast<const capk::Order*>(order));
    clock_gettime(CLOCK_MONOTONIC, &t2);
    print_timings();
}

void
order_fill(const void* order) 
{
    const capk::Order* o = (static_cast<const capk::Order*>(order));
    clock_gettime(CLOCK_MONOTONIC, &t3);
    print_timings();
}

void
order_reject(const void* order)
{
    const capk::Order* o = (static_cast<const capk::Order*>(order));
    clock_gettime(CLOCK_MONOTONIC, &t5);
    print_timings();
}


bool
receiveAggregatedBBOMarketData(zmq::socket_t* sock, capk::InstrumentBBO_t* bbo_book)
{
    if (bbo_book == NULL) {
        return false;
    }
    capkproto::instrument_bbo instrument_bbo_protobuf;
    zmq::message_t symbol_msg;
    zmq::message_t tick_msg;
    assert(sock);
    bool rc;
    pan::log_INFORMATIONAL("receiveAggregatedBBOMarketData()");
    rc = sock->recv(&symbol_msg, ZMQ_NOBLOCK);
    assert(rc);
    rc = sock->recv(&tick_msg, ZMQ_NOBLOCK);
    assert(rc);
    instrument_bbo_protobuf.ParseFromArray(tick_msg.data(), tick_msg.size());
#ifdef LOG
    //pan::log_DEBUG("MD dump raw:", pan::blob(tick_msg.data(), tick_msg.size()), 
    //        "[", pan::integer(tick_msg.size()), "]");
    //pan::log_DEBUG("MD dump prb:", instrument_bbo_protobuf.DebugString());
#endif

    // TODO FIX THIS to be int id for mic rather than string	
    // OK - 20120717
    if (instrument_bbo_protobuf.symbol() == "EUR/USD") {

#ifdef LOG
        pan::log_DEBUG("Received market data:\n", 
                instrument_bbo_protobuf.symbol(), 
                instrument_bbo_protobuf.DebugString());
#endif

        bbo_book->bid_venue_id = instrument_bbo_protobuf.bid_venue_id();
        bbo_book->bid_price = instrument_bbo_protobuf.bid_price();
        bbo_book->bid_size = instrument_bbo_protobuf.bid_size();
        clock_gettime(CLOCK_MONOTONIC, &bbo_book->bid_last_update);

        // TODO FIX THIS to be int id for mic rather than string	
        // OK - 20120717
        bbo_book->ask_venue_id = instrument_bbo_protobuf.ask_venue_id();
        bbo_book->ask_price = instrument_bbo_protobuf.ask_price();
        bbo_book->ask_size = instrument_bbo_protobuf.ask_size();
        clock_gettime(CLOCK_MONOTONIC, &bbo_book->ask_last_update);
        return true;
    }
    return false;
}

int
init(bool use_aggregated_bbo_book)
{
    int connected_interfaces = 0;
	int zero = 0;
    ///////////////////////////////////////////////////////////////////////////
    // ORDER INTERFACE SETUP
    ///////////////////////////////////////////////////////////////////////////
    
    // create the market data mux
    ptr_market_data_mux = new capk::MarketDataMux(&ctx, 
            MD_MUX);
    // create the order entry mux
	ptr_order_mux = new capk::OrderMux(&ctx, 
            ORDER_MUX);

    int64_t order_interface_ping_timeout = 1000; //sent in us and converted to ms in call
    
    // Following code does the following:
    // 1) Request config information
    // 2) Connect order interface to all "pingable" venues
    // 3) Sends HELO to available venues
    capk::get_config_params(ctx, CONFIG_SERVER_ADDR, &all_venue_config);    
    for (int i = 0; i< all_venue_config.configs_size(); i++) {
        const capkproto::venue_configuration vc = all_venue_config.configs(i);
        capk::ClientOrderInterface* p_interface = 
            new capk::ClientOrderInterface(vc.venue_id(), 
                    &ctx, 
                    vc.order_interface_addr(),
                    vc.order_ping_addr(),
                    ORDER_MUX);
        assert(p_interface);
        p_interface->init();

        bool addOK = ptr_order_mux->addOrderInterface(p_interface, order_interface_ping_timeout);
        if (addOK == false) {
            pan::log_ALERT("Did not add interface for: ", pan::integer(vc.venue_id()));
            delete p_interface;
        }
        else {
            assert(addOK);
            if (use_aggregated_bbo_book == false) {
                pan::log_INFORMATIONAL("NOT using aggregated book - ADDING MARKET DATA for venue ", 
                        pan::integer(vc.venue_id()));
                capk::ClientMarketDataInterface* ptr_md_interface = 
                    new capk::ClientMarketDataInterface(vc.venue_id(), 
                            &ctx, 
                            vc.market_data_broadcast_addr(), 
                            MD_MUX);
                assert(ptr_md_interface);
                ptr_md_interface->init();
                addOK = ptr_market_data_mux->addMarketDataInterface(ptr_md_interface);
                assert(addOK);
                connected_interfaces += 1;
            }
        }
        use_aggregated_bbo_book = false;
    }


    ///////////////////////////////////////////////////////////////////////////
    // AGGREGATED BOOK DATA INTERFACE SETUP
    ///////////////////////////////////////////////////////////////////////////
    
    if (use_aggregated_bbo_book) {
        // get the aggregated bbo book addr in case we want to use that
        std::string aggregated_bbo_book_addr = all_venue_config.aggregated_bbo_book_addr();
        int aggregated_bbo_book_id = all_venue_config.aggregated_bbo_book_id();
        pan::log_ALERT("Using aggregated book - MARKET VENUES MAY NOT MATCH ORDER VENUES!");
#ifdef LOG 
        pan::log_DEBUG("Aggregated BBO book addr: ", aggregated_bbo_book_addr.c_str());
        pan::log_DEBUG("Aggregated BBO book id  : ", pan::integer(aggregated_bbo_book_id));
#endif
        capk::ClientMarketDataInterface* ptr_agg_book_md_interface = 
                new capk::ClientMarketDataInterface(aggregated_bbo_book_id, 
                                &ctx,
                                aggregated_bbo_book_addr,
                                MD_MUX);
        // add the interface				 
        ptr_agg_book_md_interface->init();
        bool addOK = ptr_market_data_mux->addMarketDataInterface(ptr_agg_book_md_interface);
        assert(addOK);
    }

    // Must init the mux BEFORE running it
   	// send helo msg to each order interface we're connecting to
    pan::log_DEBUG("Initializing order mux");
    bool order_mux_init_ok = ptr_order_mux->init(sid);
    if (!order_mux_init_ok) {
        pan::log_CRITICAL("Error initializing order mux");
        return -1;
    }





   
    ///////////////////////////////////////////////////////////////////////////
    // Now we've setup the muxes and bound them to the interface so we need to: 
    // 1) run the muxes
    // 2) create our socket endpoints 
    // 3) set endpoint socket options
    // 4) connect endpoints to muxes
    ///////////////////////////////////////////////////////////////////////////

    // run the market data mux
#ifdef USE_MARKET_DATA
    pan::log_INFORMATIONAL("strategy init() starting MARKET DATA thread");
    mdmux_thread = new boost::thread(boost::bind(&capk::MarketDataMux::run, ptr_market_data_mux));
#endif
	// run the order mux
    pan::log_INFORMATIONAL("strategy init() starting ORDER ENTRY thread");
	omux_thread = new boost::thread(boost::bind(&capk::OrderMux::run, ptr_order_mux));
   // connect the thread local pair socket for market data
#ifdef USE_MARKET_DATA
    pMDInterface = new zmq::socket_t(ctx, ZMQ_PAIR);
    pMDInterface->setsockopt(ZMQ_LINGER, &zero, sizeof(zero));
    assert(pMDInterface);
#endif
	// connect the thread local pair socket for order data 
	pOEInterface = new zmq::socket_t(ctx, ZMQ_PAIR);
	pOEInterface->setsockopt(ZMQ_LINGER, &zero, sizeof(zero));
	assert(pOEInterface);
#ifdef LOG
    pan::log_DEBUG("Connecting order interface socket to: ", ORDER_MUX);
    pan::log_DEBUG("Connecting market data socket to: ", MD_MUX);
#endif

    om = new capk::OrderManager(pOEInterface, sid);
    om->set_callback_order_fill(order_fill);
    om->set_callback_order_new(order_new);
    om->set_callback_order_reject(order_reject);

#ifdef LOG
    pan::log_DEBUG("Sleeping 2");
#endif
    sleep(2);

    // establish connection from socket endpoint to interfaces
    try {
#ifdef USE_MARKET_DATA
        pMDInterface->connect(MD_MUX);
#endif
    	pOEInterface->connect(ORDER_MUX);
    }
    catch(zmq::error_t err) {
        pan::log_CRITICAL("EXCEPTION connecting market data mux: ", 
                err.what(),
                " (", 
                pan::integer(err.num()), 
                ") - are market data and order interfaces up?");
        return -1;
    }

#ifdef LOG
    pan::log_DEBUG("Sleeping 2");
#endif
    sleep(2);
    

    if (ptr_order_mux->get_num_interfaces() <= 0) {
        pan::log_CRITICAL("No order interfaces installed - can't do anything!");
        return -1;
    } 

#if 0
#ifdef LOG
    pan::log_DEBUG("Saying HELO to each venue");
#endif
    // KTK TODO - this should really only say helo to venues we're already connected to
    for (int i = 0; i< all_venue_config.configs_size(); i++) {
        const capkproto::venue_configuration vc = all_venue_config.configs(i);
#ifdef LOG
        pan::log_DEBUG("Sending HELO for id: ", pan::integer(vc.venue_id()));
#endif
        //capk::snd_HELOs(pOEInterface, sid, vc.venue_id(), 3000); 
        capk::snd_HELO(pOEInterface, sid, vc.venue_id()); 
    }
#endif

    return 0;

}


void
test_reject_new(const char* symbol, 
        const capk::Side_t side)
{
     static bool order_sent = false;
     double qty = 999999999999;
     double price = 0.0;
    // hitting the bid - we are selling
    if (side == capk::BID) {
        if (bbo_book.bid_price == capk::NO_BID) {
            return;
        }
        else if (!order_sent) {
            order_sent = true;
            clock_gettime(CLOCK_MONOTONIC, &t4);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            std::cout << bbo_book.bid_price << std::endl;
            price = bbo_book.bid_price - 0.005;
            capk::order_id_t ask_id = om->send_new_order(bbo_book.bid_venue_id, symbol, capk::ASK, price, qty);
            pan::log_DEBUG("test_reject_new - sent new ask:", 
                    " venue_id=", pan::integer(bbo_book.bid_venue_id), 
                    " symbol=", symbol,
                    " side=", pan::integer(capk::ASK), 
                    " price=", pan::real(price), 
                    " qty=", pan::real(qty));
        }
    }
    else if (side == capk::ASK) {
        if ( bbo_book.ask_price == capk::NO_ASK) {
            return;
        }
        else if (!order_sent) {
            order_sent = true;
            clock_gettime(CLOCK_MONOTONIC, &t4);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            price = bbo_book.ask_price + 0.005;
            capk::order_id_t bid_id = om->send_new_order(bbo_book.ask_venue_id, symbol, capk::BID, price, qty);
            pan::log_DEBUG("test_reject_new - sent new bid:", 
                    " venue_id=", pan::integer(bbo_book.bid_venue_id), 
                    " symbol=", symbol,
                    " side=", pan::integer(capk::BID), 
                    " price=", pan::real(price), 
                    " qty=", pan::real(qty));
        }
    }
    else {
        pan::log_ALERT("test_reject_new - invalid side specified");
    }
}

capk::order_id_t
send_order(capk::venue_id_t venue_id, 
        const char* symbol,
        const capk::Side_t side,
        const double price, 
        const double qty)
{
    if (om) {
        return om->send_new_order(venue_id, symbol, side, price, qty);
    }
    capk::order_id_t o(false);
    return o;
}

capk::order_id_t
send_cancel(capk::venue_id_t venue_id, 
        const capk::order_id_t& orig_cl_order_id,
        const char* symbol,
        const capk::Side_t side, 
        const double qty)
{
    if (om) {
        return om->send_cancel(venue_id, orig_cl_order_id, symbol, side, qty);
    }
    capk::order_id_t o(false);
    return o;
}

// Enters new order away from market 
// and then immediately cancels it
void
test_cancel(capk::venue_id_t venue_id, 
        const char* symbol, 
        const capk::Side_t side,
        const double price, 
        const double qty)
{
    static bool test_complete = false;
    if (!test_complete) {

        capk::order_id_t noid1 = send_order(venue_id, symbol, side, price, qty);    
        if (noid1.is_empty()) {
            pan::log_ALERT("ERROR sending new order - order_id is empty - is order interface valid?");
        }
        capk::order_id_t coid1;
        capk::Order new_order;

        bool found_order = om->get_order(noid1, new_order);
        if (!found_order) {
            capk::uuidbuf_t noid1buf;
            noid1.c_str(noid1buf);
            pan::log_CRITICAL("get_order failed for order id(1): ", noid1buf);
        }
        else {
            std::cout << "test_cancel sent new request: " << new_order << std::endl;
            std::cout << "SENDING CANCEL" << std::endl;
            coid1 = send_cancel(venue_id, noid1, symbol, side, qty);
            if (noid1.is_empty()) {
                pan::log_ALERT("ERROR sending new order - order_id is empty - is order interface valid?");
            }
        }
        capk::Order cancel_order;

        found_order = om->get_order(coid1, cancel_order);
        if (!found_order) {
            capk::uuidbuf_t coid1buf;
            coid1.c_str(coid1buf);
            pan::log_CRITICAL("get_order failed for order id(2): ", coid1buf);
        }
        else {
            std::cout << "test_cancel sent cancel request: " << cancel_order << std::endl;
        }
        test_complete = true;
    }
}

// Hits the best bid or offer whichever is specified 
// on whichever venue has the BBO for the specified symbol
// at the time the request is executed
void
test_hit_single(const char* symbol,
        const double qty, 
        const capk::Side_t side)
{
    static bool order_sent = false;
    // hitting the bid - we are selling
    if (side == capk::BID) {
        if (bbo_book.bid_price == capk::NO_BID ||
                qty > bbo_book.bid_size) { 
                //bbo_book.bid_venue_id == 0) {

            //pan::log_DEBUG("test_hit_single - either insufficient qty on bid or price invalid from market");
            //pan::log_DEBUG("book: ", 
                    //pan::real(bbo_book.bid_price), 
                    //" ", 
                    //pan::real(bbo_book.bid_size), 
                    //"@", 
                    //pan::real(bbo_book.ask_price),
                    //" ", 
                    //pan::real(bbo_book.ask_size));
            return;
        }
        else if (!order_sent) {
            order_sent = true;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            std::cout << "t1: " << t1 << std::endl;
            capk::order_id_t ask_id = om->send_new_order(bbo_book.bid_venue_id, symbol, capk::ASK, bbo_book.bid_price, qty);
            pan::log_DEBUG("test_hit_single - sent new ask:", 
                    " venue_id=", pan::integer(bbo_book.bid_venue_id), 
                    " symbol=", symbol,
                    " side=", pan::integer(capk::ASK), 
                    " price=", pan::real(bbo_book.bid_price), 
                    " qty=", pan::real(qty));
        }
    }
    // lift the ask - we are buying
    else if (side == capk::ASK) {
        if (bbo_book.ask_price == capk::NO_ASK || 
                qty > bbo_book.ask_size ||
                bbo_book.ask_venue_id == 0) {
            //pan::log_DEBUG("test_hit_single - either insufficient qty on ask or price invalid from market");
            return;
        }
        else if (!order_sent) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            capk::order_id_t bid_id = om->send_new_order(bbo_book.ask_venue_id, symbol, capk::BID, bbo_book.ask_price, qty);
            pan::log_DEBUG("test_hit_single - sent new bid:", 
                    "venue_id=", pan::integer(bbo_book.ask_venue_id), 
                    "symbol=", symbol,
                    "side=", pan::integer(capk::BID), 
                    "price=", pan::real(bbo_book.ask_price), 
                    "qty=", pan::real(qty));
            order_sent = true;
        }

    }
    else {
        pan::log_ALERT("test_hit_single - invalid side specified");
    }
}

void
evaluate()
{
    //test_hit_single("EUR/USD", 233000, capk::BID);
    //test_reject_new("EUR/USD", capk::BID);
    test_cancel(327878, "EUR/USD", capk::ASK, 1.44000, 432000);
}

int
main(int argc, char **argv)
{
    InstrumentBBO_init(&bbo_book);
	s_catch_signals();
	GOOGLE_PROTOBUF_VERIFY_VERSION;
    int retOK;
	retOK = sid.parse(STRATEGY_ID);
    assert(retOK == 0);
    pan::log_NOTICE("This strategy id: ", pan::blob(sid.get_uuid(), sid.size()));
    std::string logFileName = createTimestampedLogFilename("test_strategy");
    pan::log_NOTICE("Creating log file: ", logFileName.c_str());
	logging_init(logFileName.c_str());

    // program options
    bool use_aggregated_bbo_book = false;
    po::options_description desc("Allowed options");
    desc.add_options() 
        ("help", "this msg")
        ("aggbbo", "use aggregated BBO book for market data");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }
    if (vm.count("aggbbo")) {
        pan::log_NOTICE("Using aggregated BBO book for market data - not connecting direct");
        use_aggregated_bbo_book = true;
    }

// TESTING 
    capk::Order o;
    pan::log_INFORMATIONAL("oooooooooooooooooooooooooooooooooo> BEGIN ORDER");
    std::cout << o << std::endl;
    pan::log_INFORMATIONAL("oooooooooooooooooooooooooooooooooo> END ORDER");

// TESTING 
    
    // init() does three things
    // 0) get config settings
    // 1) ping order_engines and let mux connect to all order engines
    // 2) let mux connect to all market data venues 
    retOK = init(use_aggregated_bbo_book);
    assert(retOK == 0);
    if (retOK != 0) {
        pan::log_CRITICAL("Initialization failed - shutting down.");
        if (ptr_order_mux) { ptr_order_mux->stop(); }
        if (ptr_market_data_mux) { ptr_market_data_mux->stop(); }
        if (omux_thread != NULL) { omux_thread->join(); }
        if (mdmux_thread != NULL) { mdmux_thread->join(); }
        exit(-1);
    }
  
    // setup items to poll - only two endpoint pair sockets 
    zmq::pollitem_t pollItems[] = {
        /* { socket, fd, events, revents} */
        {*pOEInterface, NULL, ZMQ_POLLIN, 0},
#ifdef USE_MARKET_DATA
        {*pMDInterface, NULL, ZMQ_POLLIN, 0}
#endif
    };
    // start the polling loop
    while (1 && s_interrupted != 1) {
        /* N.B
         * DO NOT USE THE C++ version of poll since this will throw
         * an exception when the spurious EINTR is returned. Simply
         * check for it here, trap it, and move on.
         */
        //retOK = zmq::poll(pollItems, 2, -1);
        retOK = zmq_poll(pollItems, (sizeof(pollItems)/sizeof(zmq::pollitem_t)), -1);
        if (retOK == -1 && zmq_errno() == EINTR) {
            //pan::log_ALERT("EINTR received - FILE: ", __FILE__, " LINE: ", pan::integer(__LINE__));
            continue;
        }
        // receive market data
        if (pollItems[1].revents && ZMQ_POLLIN) {
            //pan::log_DEBUG("RECEIVING MARKET DATA");
            if (use_aggregated_bbo_book) {
                receiveAggregatedBBOMarketData(pMDInterface, &bbo_book);
            }
            else {
                receiveVenueBBOMarketData(pMDInterface, &bbo_book);
            }
            evaluate();

        }
        // receive order data
        else if (pollItems[0].revents && ZMQ_POLLIN) {
            //pan::log_DEBUG("RECEIVING ORDER DATA");
            om->receiveOrder(pOEInterface);
        }
    }

}




