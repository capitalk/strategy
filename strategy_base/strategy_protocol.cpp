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
	pan::log_DEBUG("Sending venue_id (async)");
	zmq::message_t venue_id_msg(sizeof(venue_id));
	memcpy(venue_id_msg.data(), &venue_id, sizeof(venue_id));
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	pan::log_DEBUG("sending HELO msg type (async)");
	zmq::message_t msg_helo(sizeof(capk::STRATEGY_HELO));
	memcpy(msg_helo.data(), &capk::STRATEGY_HELO, sizeof(capk::STRATEGY_HELO));
	rc = order_interface->send(msg_helo, ZMQ_SNDMORE);

	pan::log_DEBUG("sending HELO msg body (async)");
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

int
rcv_HELO(zmq::socket_t* direct_order_interface, 
        const capk::venue_id_t& vid)
{
    capk::msg_t msg_type = -1;
    capk::venue_id_t venue_id = capk::kNULL_VENUE_ID;
    int64_t more = 0;
    size_t more_size = sizeof(more);
    do {

        zmq::message_t msg_type_frame;
        direct_order_interface->recv(&msg_type_frame, 0);
        msg_type = (*(static_cast<capk::msg_t*>(msg_type_frame.data())));
        pan::log_DEBUG("rcv_HELO received msg type: ", 
                pan::integer(msg_type));


        zmq::message_t msg_data_frame;
        direct_order_interface->recv(&msg_data_frame, 0);
        pan::log_DEBUG("rcv_HELO received msg data: ", 
                pan::blob(msg_data_frame.data(), msg_data_frame.size()));
        venue_id = (*(static_cast<capk::venue_id_t*>(msg_type_frame.data())));

        direct_order_interface->getsockopt(ZMQ_RCVMORE, &more, &more_size);

        if (more != 0) {
            return -1;
        }

    } while ( more != 0);
    
    if (venue_id == vid) {
        pan::log_DEBUG("VENUE_IDS MATCH!!!!");
    }
    return 0; 
}

/* 
 * Send SYNCHRONOUS HELO msg - above is _asynchronous_
 */
int 
snd_HELO(zmq::socket_t* direct_order_interface, 
        const char* interface_addr, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venue_id) {

	// Send the HELO msg to set the route
   
	bool rc;
	pan::log_DEBUG("sending HELO msg type (sync)");
	zmq::message_t msg_helo(sizeof(capk::STRATEGY_HELO));
	memcpy(msg_helo.data(), &capk::STRATEGY_HELO, sizeof(capk::STRATEGY_HELO));
	rc = direct_order_interface->send(msg_helo, ZMQ_SNDMORE);

	pan::log_DEBUG("sending HELO msg body (sync)", pan::blob(strategy_id.get_uuid(), strategy_id.size()));
	zmq::message_t msg_sid(strategy_id.size());
	memcpy(msg_sid.data(), strategy_id.uuid(), strategy_id.size());
	rc = direct_order_interface->send(msg_sid, 0);

    zmq::pollitem_t poll_items[] = {
        {*direct_order_interface, NULL, ZMQ_POLLIN, 0}
    };
    int ret = -1;
    while (1) {
        ret = zmq::poll(poll_items, 1, 10000000);
        if (ret == -1 || ret == 0) {
            return -1;
        }
        if (poll_items[0].revents & ZMQ_POLLIN) {
            int rcvOK = rcv_HELO(direct_order_interface, venue_id);
            assert(rcvOK == 0);
        }
    }
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
	uuidbuf_t oidbuf;
	pan::log_DEBUG("CANCEL REPLACE: Creating order id: ", oid.c_str(oidbuf));
	ocr.set_cl_order_id(oid.uuid(), strategy_id.size());	

	size_t msgsize = ocr.ByteSize();
	assert(msgsize < sizeof(msgbuf));
	ocr.SerializeToArray(msgbuf, msgsize);	

	zmq::message_t msg(msgsize);
	memcpy(msg.data(), msgbuf, msgsize);

	// send the interface id to the mux
	zmq::message_t venue_id_msg(sizeof(venueID));
	memcpy(venue_id_msg.data(), &venueID, sizeof(venueID));
	pan::log_DEBUG("CANCEL REPLACE: Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	// send the message type 
	capk::msg_t order_cancel_replace = capk::ORDER_REPLACE;
	zmq::message_t msgtype(sizeof(order_cancel_replace));
	memcpy(msgtype.data(), &order_cancel_replace, sizeof(order_cancel_replace));
	pan::log_DEBUG("CANCEL REPLACE: Sending message type: ", pan::integer(order_cancel_replace));
	rc = order_interface->send(msgtype, ZMQ_SNDMORE);
	assert(rc == true);

	// send the strategy ID
	zmq::message_t sidframe(strategy_id.uuid(), strategy_id.size(),  NULL, NULL);
	pan::log_DEBUG("CANCEL REPLACE: Sending strategyid: ", pan::blob(sidframe.data(), sidframe.size()));
	rc = order_interface->send(sidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the order ID
	//zmq::message_t oidframe(oid.uuid(), strategy_id.size(),  NULL, NULL);
	zmq::message_t oidframe(strategy_id.size());
	memcpy(oidframe.data(), oid.uuid(), strategy_id.size());
	pan::log_DEBUG("CANCEL REPLACE: Sending orderid: ", pan::blob(oidframe.data(), oidframe.size()));
	rc = order_interface->send(oidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the data
	pan::log_DEBUG("CANCEL REPLACE: Sending cancel replace msg: ", pan::blob(msg.data(), msg.size()));
	rc = order_interface->send(msg, 0);
	assert(rc == true);
	pan::log_DEBUG("CANCEL REPLACE: Msg sent");
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
	uuidbuf_t oidbuf;
	pan::log_DEBUG("CANCEL: Creating order id: ", oid.c_str(oidbuf));
	oc.set_cl_order_id(oid.uuid(), strategy_id.size());	

	size_t msgsize = oc.ByteSize();
	assert(msgsize < sizeof(msgbuf));
	oc.SerializeToArray(msgbuf, msgsize);	

	zmq::message_t msg(msgsize);
	memcpy(msg.data(), msgbuf, msgsize);

	// send the interface id to the mux
	zmq::message_t venue_id_msg(sizeof(venueID));
	memcpy(venue_id_msg.data(), &venueID, sizeof(venueID));
	pan::log_DEBUG("CANCEL: Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(venue_id_msg, ZMQ_SNDMORE);

	// send the message type 
	capk::msg_t order_cancel = capk::ORDER_CANCEL;
	zmq::message_t msgtype(sizeof(order_cancel));
	memcpy(msgtype.data(), &order_cancel, sizeof(order_cancel));
	pan::log_DEBUG("CANCEL: Sending message type: ", pan::integer(order_cancel));
	rc = order_interface->send(msgtype, ZMQ_SNDMORE);
	assert(rc == true);

	// send the strategy ID
	zmq::message_t sidframe(strategy_id.uuid(), strategy_id.size(),  NULL, NULL);
	pan::log_DEBUG("CANCEL: Sending strategyid: ", pan::blob(sidframe.data(), sidframe.size()));
	rc = order_interface->send(sidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the order ID
	//zmq::message_t oidframe(oid.uuid(), strategy_id.size(),  NULL, NULL);
	zmq::message_t oidframe(strategy_id.size());
	memcpy(oidframe.data(), oid.uuid(), strategy_id.size());
	pan::log_DEBUG("CANCEL: Sending orderid: ", pan::blob(oidframe.data(), oidframe.size()));
	rc = order_interface->send(oidframe, ZMQ_SNDMORE);
	assert(rc == true);

	// send the data
	pan::log_DEBUG("CANCEL: Sending new order msg: ", pan::blob(msg.data(), msg.size()));
	rc = order_interface->send(msg, 0);
	assert(rc == true);
	pan::log_DEBUG("CANCEL: Msg sent");
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
	uuidbuf_t oidbuf;
	pan::log_DEBUG("Creating order id: ", oid.c_str(oidbuf));
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
	pan::log_DEBUG("Sending new order msg: ", pan::blob(msg.data(), msg.size()));
	rc = order_interface->send(msg, 0);
	assert(rc == true);
	pan::log_DEBUG("Msg sent");
}


