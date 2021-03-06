#ifndef __ORDER_MUX__
#define __ORDER_MUX__

#include <zmq.hpp>

#include <string>

#include "client_order_interface.h"

#include "utils/logging.h"
#include "utils/types.h"

namespace capk {

const size_t MAX_ORDER_ENTRY_INTERFACES = 10;

class OrderMux
{
	public: 
		OrderMux(zmq::context_t* context, 
				const std::string& inprocAddr);

		~OrderMux();

		// TODO - change to return int = num of installed interfaces?
		bool addOrderInterface(capk::ClientOrderInterface* oi, 
                const int64_t ping_timeout_us);

        bool init(const capk::strategy_id_t& sid);
		int run();
		void stop();

        inline size_t get_num_interfaces() const {
            return _oiArraySize;
        }

	private:
		void rcv_RESPONSE(zmq::socket_t* sock);
		// initializer list 
		zmq::context_t* _context;
		std::string _inprocAddr;
		size_t _oiArraySize;
		volatile bool _stopRequested;
		int64_t _msgCount;

        capk::ClientOrderInterface* _oiArray[MAX_ORDER_ENTRY_INTERFACES];
		zmq::pollitem_t* _poll_items;
		zmq::socket_t* _inproc;	// from strategy->venue
		

};
}; // namespace capk

#endif // __ORDER_MUX__
