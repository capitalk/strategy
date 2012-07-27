#include "logging.h"
#include "timing.h"

#include <zmq.hpp>
#include <signal.h>

#include "strategy_protocol.h"
#include "order_manager.h"

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
namespace po = boost::program_options;

// Global vars
const char* STRATEGY_ID =  "7020f42e-b6c6-42d1-9b1e-65d968961a06";
strategy_id_t sid;

const char* const ORDER_MUX = "inproc://order_mux";
const char* const MD_MUX = "inproc://md_mux";

/*
const venue_id_t capk::kFXCM_ID = 890778;
const venue_id_t kXCDE_ID = 908239;
const int capk::kCAPK_AGGREGATED_BOOK = 982132;

const char* capk::kFXCM_ORDER_INTERFACE_ADDR = "tcp://127.0.0.1:9999";
const char* kXCDE_ORDER_INTERFACE_ADDR = "tcp://127.0.0.1:9998";

const char* capk::kCAPK_AGGREGATED_BOOK_MD_INTERFACE_ADDR = "tcp://127.0.0.1:9000";
*/

#define MAX_MSGSIZE 256

// Global zmq context
zmq::context_t ctx(1);

// Global sockets these are PAIRS and the only two endpoints into the strategy
// order entry socket
zmq::socket_t* pOEInterface;
// market data socket
zmq::socket_t* pMDInterface;

// Hash tables and typedefs for storing order states
//typedef dense_hash_map<order_id_t, capk::Order, std::tr1::hash<order_id>, eq_order_id> order_map_t;
//typedef order_map_t::iterator order_map_iter_t;
//typedef std::pair<order_map_iter_t, bool> order_map_insert_t;
//typedef std::pair<order_id_t, capk::Order> order_map_value_t;
extern order_map_t pendingOrders;
extern order_map_t workingOrders;
extern order_map_t completedOrders;	

//void list_orders();

// Signal handler setup for ZMQ
static int s_interrupted = 0;

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


capkproto::order_cancel
query_cancel()
{
	pan::log_DEBUG("query_cancel()");
    int ret;
	capkproto::order_cancel oc;
	oc.set_strategy_id(sid.get_uuid(), sid.size());			

	// orig order id - i.e. order to cancel
	std::string str_origoid;
	std::cout << "CANCEL: Enter orig order id: " << std::endl;	
	std::cin >> str_origoid;
	order_id_t origoid;
	ret = origoid.parse(str_origoid.c_str());
    assert(ret == 0);
	oc.set_orig_order_id(origoid.get_uuid(), origoid.size());

	// symbol
	std::string str_symbol;
	std::cout << "CANCEL: Enter symbol: " << std::endl;
	std::cin >> str_symbol;
	oc.set_symbol(str_symbol);

	// side	
	std::string str_side;
	std::cout << "CANCEL: Enter side: " << std::endl;
	std::cin >> str_side;
	if (str_side == "B" || str_side == "b" || str_side == "0") {
		oc.set_side(capkproto::BID);
	}
	if (str_side == "S" || str_side == "s" || str_side == "1") {
		oc.set_side(capkproto::ASK);
	}
	
	// quantity
	double d_quantity;
	std::cout << "CANCEL: Enter order quantity: " << std::endl;
	std::cin >> d_quantity;
	if (d_quantity > 0) {	
		oc.set_order_qty(d_quantity);
	}
	
	pan::log_DEBUG("CANCEL: Created message [", pan::integer(oc.ByteSize()), "]\n",  oc.DebugString(), "\n");

	return oc;
}

capkproto::order_cancel_replace
query_cancel_replace()
{
	pan::log_DEBUG("query_cancel_replace()");
    int ret;
	
	capkproto::order_cancel_replace ocr;
	ocr.set_strategy_id(sid.get_uuid(), sid.size());			

	// orig order id - i.e. order to cancel and replace
	std::string str_origoid;
	std::cout << "CANCEL REPLACE: Enter orig order id: " << std::endl;	
	std::cin >> str_origoid;
	order_id_t origoid;
	ret = origoid.parse(str_origoid.c_str());
    assert(ret == 0);
	ocr.set_orig_order_id(origoid.get_uuid(), origoid.size());

	// order type
	ocr.set_ord_type(capkproto::LIM);

	// symbol
	std::string str_symbol;
	std::cout << "CANCEL REPLACE: Enter symbol: " << std::endl;
	std::cin >> str_symbol;
	ocr.set_symbol(str_symbol);

	// side	
	std::string str_side;
	std::cout << "CANCEL REPLACE: Enter side: " << std::endl;
	std::cin >> str_side;
	if (str_side == "B" || str_side == "b" || str_side == "0") {
		ocr.set_side(capkproto::BID);
	}
	if (str_side == "S" || str_side == "s" || str_side == "1") {
		ocr.set_side(capkproto::ASK);
	}

	// price 
	double d_price; 
	std::cout << "Enter order price: " << std::endl;
	std::cin >> d_price;
	ocr.set_price(d_price);
	
	// quantity
	double d_quantity;
	std::cout << "CANCEL REPLACE: Enter order quantity: " << std::endl;
	std::cin >> d_quantity;
	if (d_quantity > 0) {	
		ocr.set_order_qty(d_quantity);
	}
	
	pan::log_DEBUG("CANCEL REPLACE: Created message [", pan::integer(ocr.ByteSize()), "]\n",  ocr.DebugString(), "\n");

	return ocr;
}


capkproto::new_order_single
create_order(const char* symbol, 
				capk::Side_t side,
				double quantity,
				double price) 
{

	pan::log_DEBUG("create_order()");
	capkproto::new_order_single nos;
	nos.set_strategy_id(sid.get_uuid(), sid.size());
	nos.set_symbol(symbol);
	if (side == capk::BID) {
		nos.set_side(capkproto::BID);
	}
	else if (side == capk::ASK) {
		nos.set_side(capkproto::ASK);
	}
	nos.set_order_qty(quantity);
	nos.set_price(price);

	nos.set_ord_type(capkproto::LIM);
	nos.set_time_in_force(capkproto::GFD);
	return nos;
}	

capkproto::new_order_single
query_order()
{
	pan::log_DEBUG("query_order()");

	
	capkproto::new_order_single nos;
	nos.set_strategy_id(sid.get_uuid(), sid.size());			
    pan::log_DEBUG("SETTING SID IN NOS: ", pan::blob(sid.get_uuid(), sid.size()));

	// symbol
	std::string str_symbol;
	std::cout << "Enter symbol: " << std::endl;
	std::cin >> str_symbol;
	nos.set_symbol(str_symbol);

	// side	
	std::string str_side;
	std::cout << "Enter side: " << std::endl;
	std::cin >> str_side;
	if (str_side == "B" || str_side == "b" || str_side == "0") {
		nos.set_side(capkproto::BID);
	}
	if (str_side == "S" || str_side == "s" || str_side == "1") {
		nos.set_side(capkproto::ASK);
	}
	
	// quantity
	double d_quantity;
	std::cout << "Enter order quantity: " << std::endl;
	std::cin >> d_quantity;
	if (d_quantity > 0) {	
		nos.set_order_qty(d_quantity);
	}
	
	// order type - LIMIT only for now
	nos.set_ord_type(capkproto::LIM);

	// price 
	double d_price; 
	std::cout << "Enter order price: " << std::endl;
	std::cin >> d_price;
	nos.set_price(d_price);
	
	// set tif - GFD only for now
	nos.set_time_in_force(capkproto::GFD);

	// account 
/*
	std::string str_account; 
	std::cout << "Enter account: " << std::endl;
	std::cin >> str_account;
	nos.set_account(str_account);
*/

	pan::log_DEBUG("Created message [", pan::integer(nos.ByteSize()), "]\n",  nos.DebugString(), "\n");

	return nos;
}


bool
receiveBBOMarketData(zmq::socket_t* sock)
{
    capkproto::instrument_bbo instrument_bbo_protobuf;
    zmq::message_t tickmsg;
    assert(sock);
    bool rc;
    pan::log_DEBUG("receiveBBOMarketData()");
    rc = sock->recv(&tickmsg, ZMQ_NOBLOCK);
    assert(rc);
    instrument_bbo_protobuf.ParseFromArray(tickmsg.data(), tickmsg.size());
    capk::MultiMarketBBO_t bbo_book;

    // TODO FIX THIS to be int id for mic rather than string	
    // OK - 20120717
    if (instrument_bbo_protobuf.symbol() == "EUR/USD") {

        pan::log_DEBUG("Received market data:\n", 
                instrument_bbo_protobuf.symbol(), 
                instrument_bbo_protobuf.DebugString());

        bbo_book.bid_venue_id = instrument_bbo_protobuf.bb_venue_id();
        bbo_book.bid_price = instrument_bbo_protobuf.bb_price();
        bbo_book.bid_size = instrument_bbo_protobuf.bb_size();
        clock_gettime(CLOCK_MONOTONIC, &bbo_book.bid_last_update);

        // TODO FIX THIS to be int id for mic rather than string	
        // OK - 20120717
        bbo_book.ask_venue_id = instrument_bbo_protobuf.ba_venue_id();
        bbo_book.ask_price = instrument_bbo_protobuf.ba_price();
        bbo_book.ask_size = instrument_bbo_protobuf.ba_size();
        clock_gettime(CLOCK_MONOTONIC, &bbo_book.ask_last_update);
        return true;
    }
    return false;
}

int
init()
{
	int zero = 0;
    ///////////////////////////////////////////////////////////////////////////
    // ORDER INTERFACE SETUP
    ///////////////////////////////////////////////////////////////////////////
    // Set the empty keys for storing orders in dense_hash_map
	order_id_t oidEmpty("");
    pendingOrders.set_empty_key(oidEmpty);
    workingOrders.set_empty_key(oidEmpty);
    completedOrders.set_empty_key(oidEmpty);

    // Set the deleted key
    order_id_t oidDeleted("1");
    pendingOrders.set_deleted_key(oidDeleted);
    workingOrders.set_deleted_key(oidDeleted);
    completedOrders.set_deleted_key(oidDeleted);


    // create the market mux and add order interfaces
	OrderMux* ptr_order_mux = new OrderMux(&ctx, 
				 ORDER_MUX);

    capk::ClientOrderInterface* ptr_fxcm_order_interface 
        = new capk::ClientOrderInterface(capk::kFXCM_VENUE_ID, 
								&ctx, 
								capk::kFXCM_ORDER_INTERFACE_ADDR,	
								ORDER_MUX);

	//capk::ClientOrderInterface if_XCDE(kXCDE_VENUE_ID, 
								//&ctx, 
								//kXCDE_ORDER_INTERFACE_ADDR,
								//ORDER_MUX);
	
	// add interfaces
	ptr_fxcm_order_interface->init();
	bool addOK = ptr_order_mux->addOrderInterface(ptr_fxcm_order_interface);
    assert(addOK);
	// run the order mux
	boost::thread* t0 = new boost::thread(boost::bind(&OrderMux::run, ptr_order_mux));
	sleep(2);
	// connect the thread local pair socket for order data 
	pOEInterface = new zmq::socket_t(ctx, ZMQ_PAIR);
	pOEInterface->setsockopt(ZMQ_LINGER, &zero, sizeof(zero));
	assert(pOEInterface);
    pan::log_DEBUG("Connecting order interface socket to: ", ORDER_MUX);
    try {
    	pOEInterface->connect(ORDER_MUX);
    }
    catch(zmq::error_t err) {
        std::cerr << "EXCEPTION MUTHAFUCKA(1)! " <<  err.what() << "(" << err.num() << ")" << std::endl;
    }

	// send helo msg to each exchange we're connecting to
    // KTK TODO - SHOULD WAIT FOR ACK!!!!
	snd_HELO(pOEInterface, sid, capk::kFXCM_VENUE_ID); 
	//snd_HELO(pOEInterface, sid, kXCDE_VENUE_ID); 
  
 
    
    
    ///////////////////////////////////////////////////////////////////////////
    // MARKET DATA INTERFACE SETUP
    ///////////////////////////////////////////////////////////////////////////
    // create the market data mux
    MarketDataMux* ptr_market_data_mux = new MarketDataMux(&ctx, 
                        MD_MUX);
    // TODO differentiate between bbo stream and depth
    ClientMarketDataInterface* ptr_agg_book_md_interface = 
            new ClientMarketDataInterface(capk::kCAPK_AGGREGATED_BOOK, 
                                &ctx,
                                capk::kCAPK_AGGREGATED_BOOK_MD_INTERFACE_ADDR,
                                MD_MUX);
    // add the interface				 
    ptr_agg_book_md_interface->init();
    addOK = ptr_market_data_mux->addMarketDataInterface(ptr_agg_book_md_interface);
    assert(addOK);
    // run the market data mux
    boost::thread* t1 = new boost::thread(boost::bind(&MarketDataMux::run, ptr_market_data_mux));
    sleep(2);
    // connect the thread local pair socket for market data
    pMDInterface = new zmq::socket_t(ctx, ZMQ_PAIR);
    pMDInterface->setsockopt(ZMQ_LINGER, &zero, sizeof(zero));
    assert(pMDInterface);
    pan::log_DEBUG("Connecting market data socket to: ", MD_MUX);
    try {
        pMDInterface->connect(MD_MUX);
    }
    catch(zmq::error_t err) {
        std::cerr << "EXCEPTION MUTHAFUCKA(2)!" <<  err.what() << "(" << err.num() << ")" << std::endl;
    }

    return 0;

}

int
main(int argc, char **argv)
{
	s_catch_signals();
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	assert(sid.parse(STRATEGY_ID) == 0);
    pan::log_DEBUG("MY STRATEGY_ID IS: ", pan::blob(sid.get_uuid(), sid.size()));
    std::string logFileName = createTimestampedLogFilename("test_strategy");
	logging_init(logFileName.c_str());

    // program options
    bool runInteractive = false;
    po::options_description desc("Allowed options");
    desc.add_options() 
        ("help", "this msg")
        ("i", "interactive mode - no MARKET DATA")
    ;
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    if (vm.count("i")) {
        pan::log_NOTICE("Running interactive");
        runInteractive = true;
    }

    int initOK = init();
    pan::log_DEBUG("init() complete");
    assert(initOK == 0);
    bool rc;
    int ret;
  
    // setup items to poll - only two endpoint pair sockets 
    zmq::pollitem_t pollItems[] = {
        /* { socket, fd, events, revents} */
        {*pMDInterface, NULL, ZMQ_POLLIN, 0},
        {*pOEInterface, NULL, ZMQ_POLLIN, 0}
    };

    if (runInteractive == false) {
        // start the polling loop
        while (1 && s_interrupted != 1) {
            //pan::log_DEBUG("Polling pair sockets in app thread");
            ret = zmq::poll(pollItems, 2, -1);
            // receive market data
            if (pollItems[0].revents && ZMQ_POLLIN) {
                //pan::log_DEBUG("RECEIVING MARKET DATA");
                receiveBBOMarketData(pMDInterface);
            }
            else if (pollItems[1].revents && ZMQ_POLLIN) {
                //pan::log_DEBUG("RECEIVING ORDER DATA");
                receiveOrder(pOEInterface);
            }
        }
    }
    else {
        // setup items to poll - only two endpoint pair sockets 
        // we don't get market data in the interactive scenario
        zmq::pollitem_t pollItems[] = {
            /* { socket, fd, events, revents} */
            {*pOEInterface, NULL, ZMQ_POLLIN, 0},
            {NULL, 0, ZMQ_POLLIN, 0}
        };


        // start the polling loop
        bool shouldPrompt = true;;
        char action;
        int ret;
        while (1 && s_interrupted != 1) {
            //pan::log_DEBUG("APP Polling pair sockets in app thread");
            ret = zmq::poll(pollItems, 2, 1000000);
            // receive market data
            if (shouldPrompt) {
                std::cout << "Enter action (n=new; c=cancel; r=replace; q=quit; l=list ): " << std::endl;
                shouldPrompt = false;
            }

            if (pollItems[0].revents && ZMQ_POLLIN) {
                //pan::log_DEBUG("RECEIVING ORDER DATA");
                receiveOrder(pOEInterface);
            }
            if (pollItems[1].revents && ZMQ_POLLIN) {
                char buf[16];
                char action;
                //fgets(action, sizeof(buf), stdin);
                action = fgetc(stdin);
                pan::log_DEBUG("ACTION RECEIVED: ", pan::character(action));
                switch (action) {
                case '\n': break;
                case '\r': break;
                case 'n':
                {
                    capkproto::new_order_single order =  query_order();
                    snd_NEW_ORDER(pOEInterface, 
                            sid, 
                            capk::kFXCM_VENUE_ID, 
                            order);
                    break;
                }
                case 'c': 
                {
                    capkproto::order_cancel cancel = query_cancel();
                    snd_ORDER_CANCEL(pOEInterface, 
                            sid, 
                            capk::kFXCM_VENUE_ID, 
                            cancel);
                    break;
                }
                case 'r': 
                {
                    capkproto::order_cancel_replace cancel_replace = query_cancel_replace();
                    snd_ORDER_CANCEL_REPLACE(pOEInterface, 
                            sid, 
                            capk::kFXCM_VENUE_ID, 
                            cancel_replace);
                    break;
                }	
                case 'q': 
                {
                    s_interrupted = 1;
                    break;
                }
                case 'l': 
                {
                    list_orders();
                    break;
                }
                default: 
                    std::cerr << "Invalid action:" << action << std::endl;
                    break;
            }

            shouldPrompt = true;

            }
        }
    }
}




