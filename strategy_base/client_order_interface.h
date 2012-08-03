#ifndef __CLIENT_ORDER_INTERFACE__
#define __CLIENT_ORDER_INTERFACE__

#include <zmq.hpp>
#include <string>

namespace capk {
class ClientOrderInterface
{
	public:
		ClientOrderInterface(const int venueID,
							zmq::context_t* context, 
							const std::string& interfaceAddr, 
							const std::string& pingAddr, 
							const std::string& inprocAddr):
		_venueID(venueID),
		_context(context), 
		_interfaceAddr(interfaceAddr),
		_pingAddr(pingAddr),
		_inprocAddr(inprocAddr),
		_stopRequested(false),
		_initComplete(false)
		{}
		
		~ClientOrderInterface();

		void init();
		//int run();
		//void stop();
		
        inline zmq::context_t* getContext() const { return _context;}
		inline const std::string& getInterfaceAddr() const { return _interfaceAddr;}
		inline const std::string& getPingAddr() const { return _pingAddr;}
		inline const std::string& getInprocAddr() const { return _inprocAddr;}
		inline const int getVenueID() const { return _venueID;}
		inline zmq::socket_t* getInterfaceSocket() { return _interface;}
		inline zmq::socket_t* getInprocSocket() { return _inproc;}

	private:
		int _venueID;
		zmq::context_t* _context;

		std::string _interfaceAddr;
		std::string _pingAddr;
		zmq::socket_t* _interface;

		std::string _inprocAddr;
		zmq::socket_t* _inproc;

		volatile bool _stopRequested;
		int64_t _msgCount;
		bool _initComplete;

};

} // namespace capk

#endif // __CLIENT_ORDER_INTERFACE__
