#include "client_market_data_interface.h"

#include "utils/logging.h"

#include <zmq.hpp>

namespace capk {

ClientMarketDataInterface::~ClientMarketDataInterface()
{
    pan::log_DEBUG("ClientMarketDataInterface::~ClientMarketDataInterface()");

	if (_interface) {
		delete _interface;
	}
}

/*
void
ClientMarketDataInterface::stop()
{
	_stopRequested = true;
}
*/

void 
ClientMarketDataInterface::unsetSubscriptionFilter(const char* filter)
{
    if (filter) {
        _interface->setsockopt(ZMQ_UNSUBSCRIBE, filter, strlen(filter));
    }
}

void 
ClientMarketDataInterface::setSubscriptionFilter(const char* filter)
{
    if (filter) {
        _interface->setsockopt(ZMQ_SUBSCRIBE, filter, strlen(filter));
    }
}

void
ClientMarketDataInterface::init()
{
	/* 
	 * N.B.  
	 * Only connect the socket to the interface. Don't connect
	 * the inproc socket here since there will then be more than one DEALER
	 * connected to the inproc socket here and messages _received_
	 * from the interface will then be round-robined to these and 
	 * consequently not returned to the application thread
	 */
	if (_initComplete == false) {
		assert(_context != NULL);
		assert(_interfaceAddr.c_str() != NULL);
		try {
			_interface = new zmq::socket_t(*_context, ZMQ_SUB);
			assert(_interface);
			_interface->connect(_interfaceAddr.c_str());
			const char* filter = "";
            setSubscriptionFilter(filter);
		}
		catch (std::exception& e) {
			pan::log_CRITICAL("EXCEPTION: ", __FILE__, pan::integer(__LINE__), " ", e.what());
		}
	}

	_initComplete = true;
}

bool
ClientMarketDataInterface::subscribe(const char* sub)
{
    if (_interface && sub && *sub) {   
        _interface->setsockopt(ZMQ_SUBSCRIBE, sub, strlen(sub));
        return true;
    }
    return false;
}

bool
ClientMarketDataInterface::unsubscribe(const char* sub)
{
    if (_interface && sub && *sub) {   
        _interface->setsockopt(ZMQ_UNSUBSCRIBE, sub, strlen(sub));
        return true;
    }
    return false;
}



/* 
 * Use run() for running in a separate thread only!
 */
/*
int 
ClientMarketDataInterface::run()
{
	try {
		init();

		assert(_context != NULL);
		assert(_interfaceAddr.c_str() != NULL);
		zmq::socket_t oe(*_context, ZMQ_DEALER);
		oe.connect(_interfaceAddr.c_str());
		
		_inproc = new zmq::socket_t(*_context, ZMQ_PULL);
		assert(_inproc);
		_inproc->connect(_inprocAddr.c_str());


		zmq::pollitem_t poll_items[] = {
			{ *_interface, NULL, ZMQ_POLLIN, 0},
			{ *_inproc, NULL, ZMQ_POLLIN, 0}
		};

		bool rc;
		int ret;	
		int64_t more;
		size_t more_size = sizeof(more);
		while (1 && _stopRequested == false) {
			ret = zmq::poll(poll_items, 2, -1);
			if (ret == -1) {
				return -1;
			}		
			// interface connection 
			// incoming messages forwarded to inproc socket
			if (poll_items[0].revents & ZMQ_POLLIN) {
				_msgCount++;	
				do {
					zmq::message_t msg;
					rc = _interface->recv(&msg, 0);
					assert(rc);
					_interface->getsockopt(ZMQ_RCVMORE, &more, &more_size);
					rc = _inproc->send(msg, more ? ZMQ_SNDMORE : 0);	
				} while (more);
			}
			// inproc connection 
			// incoming messages forwarded to oe
			if (poll_items[1].revents & ZMQ_POLLIN) {
				_msgCount++;	
				do {
					zmq::message_t msg;
					rc = _inproc->recv(&msg, 0);
					assert(rc);
					_inproc->getsockopt(ZMQ_RCVMORE, &more, &more_size);
					rc = _interface->send(msg, more ? ZMQ_SNDMORE : 0);	
				} while (more);
			}
		}		
	}
	catch(std::exception& e) {
		pan::log_CRITICAL("EXCEPTION: ", __FILE__, pan::integer(__LINE__), " ", e.what());
	}
	return 0;
}
*/

}; // namespace capk
