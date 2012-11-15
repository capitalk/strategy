#include "order_mux.h"
#include "utils/msg_types.h"
#include "strategy_protocol.h"

#include "proto/execution_report.pb.h"
#include "proto/order_cancel_reject.pb.h"

namespace capk {

OrderMux::OrderMux(zmq::context_t* context, 
				const std::string& inprocAddr):
				_context(context),
				_inprocAddr(inprocAddr),
				_oiArraySize(0),
				_stopRequested(false),
				_msgCount(0),
                _inproc(NULL)
{
    memset(_oiArray, 0, sizeof(_oiArray));
};

OrderMux::~OrderMux()
{
#ifdef LOG
    pan::log_DEBUG("OrderMux::~OrderMux()");
#endif
	if (_inproc) {
		delete _inproc;
	}		
    for (size_t i = 0; i<_oiArraySize; i++) {
        delete _oiArray[i];     
        _oiArray[i] = NULL;
    }
	if (_poll_items) {
		delete [] _poll_items;
	}
}

void 
OrderMux::stop()
{
	_stopRequested = true;
}

// TODO - change to return int = num of installed interfaces
bool 
OrderMux::addOrderInterface(capk::ClientOrderInterface* oi,
        const int ping_timeout_us)
{
    if (!oi) { 
        return false;
    }

    int pingOK = PING(oi->getContext(), 
            oi->getPingAddr().c_str(), 
            ping_timeout_us);

    if (pingOK != 0) {
        pan::log_CRITICAL("PING failed to: ", 
                oi->getPingAddr().c_str(), 
                " with timeout (us): ", 
                pan::integer(ping_timeout_us), 
                " NOT ADDING INTERFACE");
    }
    else {
        //if (_oiArraySize+1 < MAX_ORDER_ENTRY_INTERFACES) {
        if (_oiArraySize < MAX_ORDER_ENTRY_INTERFACES) {
            _oiArray[_oiArraySize] = oi;	
            pan::log_DEBUG("Adding order interface: ", pan::integer(oi->getVenueID()), " [", pan::integer(_oiArraySize), "]");
            _oiArraySize++;
            return true;
        }
    }
    for (size_t i = 0; i< _oiArraySize; i++) {
        pan::log_DEBUG("_OIARRAYSIZE is  :", pan::integer((int)i));
        pan::log_DEBUG("=================>", pan::integer((int)i),":", pan::integer(_oiArray[i]->getVenueID()));
    }
    return false;
}

int	
OrderMux::run()
{
	try {
		assert(_context != NULL);

		_inproc = new zmq::socket_t(*_context, ZMQ_PAIR);
		assert(_inproc);
		pan::log_INFORMATIONAL("Binding OrderMux inproc addr: ", 
                _inprocAddr.c_str());
		_inproc->bind(_inprocAddr.c_str());
/*
		for (size_t i = 0; i<_oiArraySize; i++) {
			_oiArray[i]->init();					
		}
*/
		
		// 0th item in poll_items is always inproc socket
		_poll_items = new zmq::pollitem_t[_oiArraySize + 1];
		
		_poll_items[0].socket = *_inproc;
		_poll_items[0].fd = NULL;
		_poll_items[0].events = ZMQ_POLLIN;
		_poll_items[0].revents = 0;
		
		for (size_t i = 0; i < _oiArraySize; i++) {
            zmq::socket_t* order_interface = _oiArray[i]->getInterfaceSocket();
            assert(order_interface);
			_poll_items[i+1].socket = *(order_interface);
			_poll_items[i+1].fd = NULL;
			_poll_items[i+1].events = ZMQ_POLLIN;
			_poll_items[i+1].revents = 0;
		}

/*
        pan::log_DEBUG("Waiting for order interfaces...");
        int64_t  x;
        while (_oiArraySize < 1) {
            x++;
            timespec req;
            req.tv_nsec = 500;
            timespec rem;
            nanosleep(&req, &rem);
        }
*/
/*
	    if (_oiArraySize < 2) { // inproc socket is one
            pan::log_CRITICAL("NO ORDER INTERFACES INSTALLED [", 
                    pan::integer(_oiArraySize), "]");
            return -1;
        }
*/
		pan::log_INFORMATIONAL("Number of interfaces installed: ",
               pan::integer( _oiArraySize));

	
		bool rc = false;
		int ret = -1;	
		int64_t more = 0;
		size_t more_size = sizeof(more);
		while (1 && _stopRequested == false) {
			//ret = zmq::poll(_poll_items, _oiArraySize + 1, -1);
            /* N.B
             * DO NOT USE THE C++ version of poll since this will throw
             * an exception when the spurious EINTR is returned. Simply
             * check for it here, trap it, and move on.
             */
            ret = zmq_poll(_poll_items, _oiArraySize + 1, -1);
            if (ret == -1 and zmq_errno() == EINTR) {
                pan::log_ALERT("EINTR received - FILE: ", __FILE__, " LINE: ", pan::integer(__LINE__));
                continue;
            }

			// outbound orders routed to correct venue 
			if (_poll_items[0].revents & ZMQ_POLLIN) {
				_msgCount++;	
					// get the venue id so we can route
					zmq::message_t venue_id_msg;
					rc = _inproc->recv(&venue_id_msg, 0);
					assert(rc);
					// lookup the socket for the venue
					zmq::socket_t* venue_sock = NULL;
					int venue_id = *(static_cast<int*>(venue_id_msg.data()));
#ifdef LOG
					pan::log_DEBUG("OMUX (outbound) received msg for interface id: ", pan::integer(venue_id));
#endif

					size_t sockIdx;
					for (sockIdx = 0; sockIdx < _oiArraySize; sockIdx++) {
						if (_oiArray[sockIdx]->getVenueID() == venue_id) {
                            pan::log_DEBUG("++++++++++", pan::integer(sockIdx), pan::integer(venue_id));
							venue_sock = _oiArray[sockIdx]->getInterfaceSocket();
							assert(venue_sock);
#ifdef LOG
							pan::log_DEBUG("OMUX (outbound) found interface socket for id: ", pan::integer(venue_id));
							pan::log_DEBUG("OMUX (outbound) interface has the following attributes:\n", "Interface address: ", _oiArray[sockIdx]->getInterfaceAddr().c_str(), "\n", "Inproc address: ", _oiArray[sockIdx]->getInprocAddr().c_str(), "\n");
#endif
						}
					}
					if (sockIdx >= _oiArraySize) {
						pan::log_CRITICAL("OMUX (outbound) cant find interface for id: ", pan::integer(venue_id), " MSG NOT SENT!");
                        return (-1);
					}

				do {
					// recv and forward remaining frames 
					zmq::message_t msg;
					rc = _inproc->recv(&msg, 0);
					//pan::log_DEBUG("OMUX forwarding frame from inproc: ", pan::blob(msg.data(), msg.size()));
					assert(rc);		
					_inproc->getsockopt(ZMQ_RCVMORE, &more, &more_size);
					rc = venue_sock->send(msg, more ? ZMQ_SNDMORE : 0);	
				} while (more);
				//pan::log_DEBUG("OMUX finished forwarding");
			}
			else {	
			// messages returning from venue
			// don't need to be routed
				for (size_t j = 0; j<_oiArraySize; j++) {
					//pan::log_DEBUG("OMUX checking incoming messages on oiArray: ", pan::integer(j));
					if (_poll_items[j+1].revents && ZMQ_POLLIN) {
						zmq::socket_t* sock = _oiArray[j]->getInterfaceSocket();
						assert(sock);
						_msgCount++;	
						rcv_RESPONSE(sock);
					}
				}
			}		
		}
	}
	catch(zmq::error_t e) {
		pan::log_CRITICAL("EXCEPTION: ", __FILE__, pan::integer(__LINE__), " ", e.what(), " (", pan::integer(e.num()), ")");
	}	
	catch(std::exception& e) {
		pan::log_CRITICAL("EXCEPTION: ", __FILE__, pan::integer(__LINE__), " ", e.what());
	}	
	return 0;
}


void 
OrderMux::rcv_RESPONSE(zmq::socket_t* sock)
{
	int64_t more = 0;
	size_t more_size = sizeof(more);
	//pan::log_DEBUG("Entering recv loop");
	do {
		zmq::message_t msgtypeframe;
		sock->recv(&msgtypeframe, 0); 
        
		//pan::log_DEBUG("OMUX Received msgtypeframe: size=", 
						//pan::integer(msgtypeframe.size()), 
						//" data=", 
						//pan::blob(static_cast<const void*>(msgtypeframe.data()), msgtypeframe.size()));
        
		
		zmq::message_t msgframe;
		sock->recv(&msgframe, 0);
		//pan::log_DEBUG("OMUX Received msgframe: size=", 
						//pan::integer(msgframe.size()), 
						//" data=", 
						//pan::blob(static_cast<const void*>(msgframe.data()), msgframe.size()));

		

		if (*(static_cast<capk::msg_t*>(msgtypeframe.data())) == capk::STRATEGY_HELO_ACK) {
            /*
			pan::log_DEBUG("OMUX Received msg type: ", pan::integer(capk::STRATEGY_HELO_ACK),
							" - capk::STRATEGY_HELO_ACK from venue ID: ",
							pan::integer(*(static_cast<capk::venue_id_t*>(msgframe.data()))));
            */
		}
		
        else {
            //pan::log_DEBUG("OMUX sending to _inproc");
			_inproc->send(msgtypeframe, ZMQ_SNDMORE);
			_inproc->send(msgframe, 0);
        }
/* Don't inspect the protobufs here - just pass them up to the inrpoc socket 
 * and then let the application thread process the messages - more synchronous - easier
 * to digest, understand, debug */
/* 
		if (*(static_cast<capk::msg_t*>(msgtypeframe.data())) == capk::EXEC_RPT) {
			bool parseOK;
			pan::log_DEBUG("Received msg type: ", pan::integer(capk::EXEC_RPT), " - capk::EXEC_RPT");
			capkproto::execution_report er;
			parseOK = er.ParseFromArray(msgframe.data(), msgframe.size());
			assert(parseOK);
			pan::log_DEBUG(er.DebugString());
			// forward msg to application thread
			_inproc->send(msgtypeframe, ZMQ_SNDMORE);
			_inproc->send(msgframe, 0);
		}
		if (*(static_cast<capk::msg_t*>(msgtypeframe.data())) == capk::ORDER_CANCEL_REJ) {
			bool parseOK;
			pan::log_DEBUG("Received msg type: ", pan::integer(capk::ORDER_CANCEL_REJ), " - capk::ORDER_CANCEL_REJ");
			capkproto::order_cancel_reject ocr;
			parseOK = ocr.ParseFromArray(msgframe.data(), msgframe.size());
			assert(parseOK);
			pan::log_DEBUG(ocr.DebugString());
			// forward msg to application thread
			_inproc->send(msgtypeframe, ZMQ_SNDMORE);
			_inproc->send(msgframe, 0);
		}
*/
		zmq_getsockopt(*sock, ZMQ_RCVMORE, &more, &more_size);
		assert(more == 0);
	} while (more);
	//pan::log_DEBUG("OMUX Exiting recv loop");
}

}; // namespace capk

