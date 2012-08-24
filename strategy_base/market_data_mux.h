#ifndef __MARKET_DATA_MUX__
#define __MARKET_DATA_MUX__

#include <zmq.hpp>

#include <string>

#include "client_market_data_interface.h"

#include "utils/logging.h"

namespace capk {

const size_t MAX_MARKET_DATA_INTERFACES = 10;

class MarketDataMux
{
	public: 
		MarketDataMux(zmq::context_t* context, 
				const std::string& inprocAddr);

		~MarketDataMux();

		// TODO - change to return int = num of installed interfaces?
		bool addMarketDataInterface(ClientMarketDataInterface* mdi);

		int run();
		void stop();

	private:
		void rcv_RESPONSE(zmq::socket_t* sock);
		// initializer list 
		zmq::context_t* _context;
		std::string _inprocAddr;
		size_t _mdArraySize;
		volatile bool _stopRequested;
		int64_t _msgCount;

		ClientMarketDataInterface* _mdArray[MAX_MARKET_DATA_INTERFACES];
		zmq::pollitem_t* _poll_items;
		zmq::socket_t* _inproc;	// from strategy->venue
		

};
}; // namespace capk

#endif // __MARKET_DATA_MUX__
