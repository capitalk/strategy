#include "strategy_protocol.h"

/*
 * Async snd_HELO - 
 * N.B. USE THIS WHEN SENDING FROM MUX ASYNC - USE THE SYNC VERSION 
 * WHEN SENDING DIRECT!
 */
int 
snd_HELO(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venue_id) {
	// Send the HELO msg to set the route
/*
	int64_t more = 0;
	size_t more_size = sizeof(more);
*/
	bool rc;
#ifdef LOG
	pan::log_DEBUG("Sending venue_id (async)");
#endif
	zmq::message_t venue_id_msg(sizeof(venue_id));
	memcpy(venue_id_msg.data(), &venue_id, sizeof(venue_id));
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

#ifdef LOG
	pan::log_DEBUG("sending HELO msg type (async)");
#endif
	zmq::message_t msg_helo(sizeof(capk::STRATEGY_HELO));
	memcpy(msg_helo.data(), &capk::STRATEGY_HELO, sizeof(capk::STRATEGY_HELO));
	rc = order_interface->send(msg_helo, ZMQ_SNDMORE);

#ifdef LOG
	pan::log_DEBUG("sending HELO msg body (async)");
#endif
	zmq::message_t msg_sid(strategy_id.size());
	memcpy(msg_sid.data(), strategy_id.uuid(), strategy_id.size());
	rc = order_interface->send(msg_sid, 0);
/*	
	pan::log_DEBUG("waiting for HELO ACK");
	zmq::message_t msg_helo_ack;
	rc = order_interface->recv(&msg_helo_ack, 0); 
	zmq_getsockopt(*order_interface, ZMQ_RCVMORE, &more, &more_size);
	assert(more == 0);
	pan::log_DEBUG("rcvd for HELO ACK");
*/	

	return 0;
}


/* 
 * Send SYNCHRONOUS PING msg  
 */
int 
PING(zmq::context_t* pzmq_ctx, 
        const char* interface_ping_addr, 
        const int64_t poll_timeout_us)
{

    assert(pzmq_ctx);
    assert(interface_ping_addr && *interface_ping_addr);

	bool rc;
    zmq::socket_t ping_sock(*pzmq_ctx, ZMQ_REQ);  
//#ifdef LOG
	pan::log_DEBUG("snd_PING connecting to ping interface on: ",
            interface_ping_addr);
//#endif
    ping_sock.connect(interface_ping_addr);
	zmq::message_t ping_frame(sizeof(capk::PING));
	memcpy(ping_frame.data(), &capk::PING, sizeof(capk::PING));
//#ifdef LOG
    T0(a);
    pan::log_DEBUG("Sent PING msg (", to_simple_string(a).c_str(), ")");
//#endif
	rc = ping_sock.send(ping_frame, 0);

    zmq::pollitem_t poll_items[] = {
        {ping_sock, NULL, ZMQ_POLLIN, 0}
    };
    int ret = -1;
    while (1) {
        ret = zmq::poll(poll_items, 1, poll_timeout_us);
        if (ret == -1 || ret == 0) {
            return -1;
        }
        if (poll_items[0].revents & ZMQ_POLLIN) {
            zmq::message_t msg_type_frame;
            ping_sock.recv(&msg_type_frame, 0);
            capk::msg_t msg_type = (*(static_cast<capk::msg_t*>(msg_type_frame.data())));
            if (msg_type == capk::PING_ACK) {
                T0(a);
                pan::log_DEBUG("Received PING_ACK msg (", to_simple_string(a).c_str(), ")");
                return 0;
            }
            else {
                pan::log_CRITICAL("Received UNKNOWN msg type in response to PING: ", pan::integer(msg_type));
                return -1;
            }
        }
    }
	return -1;
}



void 
snd_ORDER_CANCEL_REPLACE(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venueID, 
        capkproto::order_cancel_replace& ocr) 
{
	bool rc;
	char msgbuf[MAX_MSGSIZE];

	// create an order id for this order
	order_id oid(true);
	//char oidbuf[UUID_STRLEN + 1];
#ifdef LOG
	uuidbuf_t oidbuf;
	pan::log_DEBUG("CANCEL REPLACE: Creating order id: ", oid.c_str(oidbuf));
#endif
	ocr.set_cl_order_id(oid.uuid(), strategy_id.size());	

	size_t msgsize = ocr.ByteSize();
	assert(msgsize < sizeof(msgbuf));
	ocr.SerializeToArray(msgbuf, msgsize);	

	zmq::message_t msg(msgsize);
	memcpy(msg.data(), msgbuf, msgsize);

	// send the interface id to the mux
	zmq::message_t venue_id_msg(sizeof(venueID));
	memcpy(venue_id_msg.data(), &venueID, sizeof(venueID));
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Sending venueID: ", pan::integer(venueID));
#endif
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	// send the message type 
	capk::msg_t order_cancel_replace = capk::ORDER_REPLACE;
	zmq::message_t msgtype(sizeof(order_cancel_replace));
	memcpy(msgtype.data(), &order_cancel_replace, sizeof(order_cancel_replace));
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Sending message type: ", pan::integer(order_cancel_replace));
#endif
	rc = order_interface->send(msgtype, ZMQ_SNDMORE);
	assert(rc == true);

	// send the strategy ID
	zmq::message_t sidframe(strategy_id.uuid(), strategy_id.size(),  NULL, NULL);
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Sending strategyid: ", pan::blob(sidframe.data(), sidframe.size()));
#endif
	rc = order_interface->send(sidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the order ID
	//zmq::message_t oidframe(oid.uuid(), strategy_id.size(),  NULL, NULL);
	zmq::message_t oidframe(strategy_id.size());
	memcpy(oidframe.data(), oid.uuid(), strategy_id.size());
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Sending orderid: ", pan::blob(oidframe.data(), oidframe.size()));
#endif
	rc = order_interface->send(oidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the data
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Sending cancel replace msg: ", pan::blob(msg.data(), msg.size()));
#endif
	rc = order_interface->send(msg, 0);
	assert(rc == true);
#ifdef LOG
	pan::log_DEBUG("CANCEL REPLACE: Msg sent");
#endif
}


void 
snd_ORDER_CANCEL(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venueID, 
        capkproto::order_cancel& oc) 
{
	bool rc;
	char msgbuf[MAX_MSGSIZE];

	// create an order id for this order
	order_id oid(true);
	//char oidbuf[UUID_STRLEN + 1];
#ifdef LOG
	uuidbuf_t oidbuf;
	pan::log_DEBUG("CANCEL: Creating order id: ", oid.c_str(oidbuf));
#endif
	oc.set_cl_order_id(oid.uuid(), strategy_id.size());	

	size_t msgsize = oc.ByteSize();
	assert(msgsize < sizeof(msgbuf));
	oc.SerializeToArray(msgbuf, msgsize);	

	zmq::message_t msg(msgsize);
	memcpy(msg.data(), msgbuf, msgsize);

	// send the interface id to the mux
	zmq::message_t venue_id_msg(sizeof(venueID));
	memcpy(venue_id_msg.data(), &venueID, sizeof(venueID));
#ifdef LOG
	pan::log_DEBUG("CANCEL: Sending venueID: ", pan::integer(venueID));
#endif
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	// send the message type 
	capk::msg_t order_cancel = capk::ORDER_CANCEL;
	zmq::message_t msgtype(sizeof(order_cancel));
	memcpy(msgtype.data(), &order_cancel, sizeof(order_cancel));
#ifdef LOG
	pan::log_DEBUG("CANCEL: Sending message type: ", pan::integer(order_cancel));
#endif
	rc = order_interface->send(msgtype, ZMQ_SNDMORE);
	assert(rc == true);

	// send the strategy ID
	zmq::message_t sidframe(strategy_id.uuid(), strategy_id.size(),  NULL, NULL);
#ifdef LOG
	pan::log_DEBUG("CANCEL: Sending strategyid: ", pan::blob(sidframe.data(), sidframe.size()));
#endif
	rc = order_interface->send(sidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the order ID
	//zmq::message_t oidframe(oid.uuid(), strategy_id.size(),  NULL, NULL);
	zmq::message_t oidframe(strategy_id.size());
	memcpy(oidframe.data(), oid.uuid(), strategy_id.size());
#ifdef LOG
	pan::log_DEBUG("CANCEL: Sending orderid: ", pan::blob(oidframe.data(), oidframe.size()));
#endif
	rc = order_interface->send(oidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the data
#ifdef LOG
	pan::log_DEBUG("CANCEL: Sending new order msg: ", pan::blob(msg.data(), msg.size()));
#endif
	rc = order_interface->send(msg, 0);
	assert(rc == true);
#ifdef LOG
	pan::log_DEBUG("CANCEL: Msg sent");
#endif
}


void 
snd_NEW_ORDER(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venueID, 
        capkproto::new_order_single& nos) 
{
	bool rc;
	char msgbuf[MAX_MSGSIZE];

    nos.set_venue_id(venueID);

	// create an order id for this order
	order_id oid(true);
	//char oidbuf[UUID_STRLEN + 1];
#ifdef LOG
	uuidbuf_t oidbuf;
	pan::log_DEBUG("Creating order id: ", oid.c_str(oidbuf));
#endif
	nos.set_order_id(oid.uuid(), strategy_id.size());	

	size_t msgsize = nos.ByteSize();
	assert(msgsize < sizeof(msgbuf));
	nos.SerializeToArray(msgbuf, msgsize);	

	zmq::message_t msg(msgsize);
	memcpy(msg.data(), msgbuf, msgsize);

	// send the interface id to the mux
	zmq::message_t venue_id_msg(sizeof(venueID));
	memcpy(venue_id_msg.data(), &venueID, sizeof(venueID));
	//pan::log_DEBUG("Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	// send the message type 
	capk::msg_t order_new = capk::ORDER_NEW;
	//zmq::message_t msgtype(&order_new_type, sizeof(order_new_type), NULL, NULL);
	zmq::message_t msgtype(sizeof(order_new));
	memcpy(msgtype.data(), &order_new, sizeof(order_new));
	//pan::log_DEBUG("Sending message type: ", pan::integer(order_new));
	rc = order_interface->send(msgtype, ZMQ_SNDMORE);
	assert(rc == true);

	// send the strategy ID
	zmq::message_t sidframe(strategy_id.uuid(), strategy_id.size(),  NULL, NULL);
	//pan::log_DEBUG("Sending strategyid: ", pan::blob(sidframe.data(), sidframe.size()));
	rc = order_interface->send(sidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the order ID
	//zmq::message_t oidframe(oid.uuid(), strategy_id.size(),  NULL, NULL);
	zmq::message_t oidframe(strategy_id.size());
	memcpy(oidframe.data(), oid.uuid(), strategy_id.size());
	//pan::log_DEBUG("Sending orderid: ", pan::blob(oidframe.data(), oidframe.size()));
	rc = order_interface->send(oidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the data
#ifdef LOG
	pan::log_DEBUG("Sending new order msg: ", pan::blob(msg.data(), msg.size()));
#endif
	rc = order_interface->send(msg, 0);
	assert(rc == true);
#ifdef LOG
	pan::log_DEBUG("Msg sent");
#endif
}


