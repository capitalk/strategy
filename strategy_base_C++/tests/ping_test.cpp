#include "strategy_base/strategy_protocol.h"
#include <zmq.hpp>

zmq::context_t ctx(1);

int
main()
{
    pan::log_DEBUG("START");
    int ping_ret = capk::PING(&ctx, 
            "tcp://127.0.0.1:7997", 
            3000 * 1000);
    pan::log_DEBUG("START");

}
