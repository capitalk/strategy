#include "strategy_protocol.h"

int 
snd_HELO(zmq::socket_t* order_interface, 
        strategy_id_t& strategy_id, 
        const capk::venue_id_t venueID) {
	// Send the HELO msg to set the route
/*
	int64_t more = 0;
	size_t more_size = sizeof(more);
*/
	bool rc;
	pan::log_DEBUG("Sending venueID");
	zmq::message_t iid(sizeof(venueID));
	memcpy(iid.data(), &venueID, sizeof(venueID));
	rc = order_interface->send(iid, ZMQ_SNDMORE);

	pan::log_DEBUG("sending HELO msg type");
	zmq::message_t msg_helo(sizeof(capk::STRATEGY_HELO));
	memcpy(msg_helo.data(), &capk::STRATEGY_HELO, sizeof(capk::STRATEGY_HELO));
	rc = order_interface->send(msg_helo, ZMQ_SNDMORE);

	pan::log_DEBUG("sending HELO msg body");
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
	zmq::message_t iid(sizeof(venueID));
	memcpy(iid.data(), &venueID, sizeof(venueID));
	pan::log_DEBUG("CANCEL REPLACE: Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(iid, ZMQ_SNDMORE);

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
	zmq::message_t iid(sizeof(venueID));
	memcpy(iid.data(), &venueID, sizeof(venueID));
	pan::log_DEBUG("CANCEL: Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(iid, ZMQ_SNDMORE);

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
	zmq::message_t iid(sizeof(venueID));
	memcpy(iid.data(), &venueID, sizeof(venueID));
	//pan::log_DEBUG("Sending venueID: ", pan::integer(venueID));
	rc = order_interface->send(iid, ZMQ_SNDMORE);

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


