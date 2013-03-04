#include "order.h"

namespace capk {

Order::Order() : _clOid(false), 
		_origClOid(false),
        _venue_id(false),
		_execId({0}),
        _execTransType(ExecTransType_t::NO_EXEC_TRAN),
        _ordStatus(OrdStatus_t::NO_ORD_STATUS),
        _execType(ExecType_t::NO_EXEC_TYPE),
		_symbol({0}),
        _secType({0}),
        _account({0}), 
		_side(Side_t::NO_SIDE),
		_orderQty(0),
        _ordType(0), 
		_price(0),
		_lastShares(0),
		_lastPrice(0),
		_leavesQty(0),
		_cumQty(0),
		_avgPrice(0),
        _timeInForce(0),
        _transactTime(),
        _transactTimeStr(""),
        _execInstStr(""),
        _handlInst(0),
        _ordRejReason(-1),
        _minQty(0),
        _exec_restatement_reason(-1),
        _exec_ref_id({0}),
        _state(OrderState_t::INT_STATE_UNINIT)
{
    // _transactTime()
};

Order::Order(order_id_t& oid): _clOid(oid),
		_origClOid(false),
        _venue_id(false),
		_execId({0}),
        _execTransType(ExecTransType_t::NO_EXEC_TRAN),
        _ordStatus(OrdStatus_t::NO_ORD_STATUS),
        _execType(ExecType_t::NO_EXEC_TYPE),
		_symbol({0}),
        _secType({0}),
        _account({0}), 
		_side(Side_t::NO_SIDE),
		_orderQty(0),
        _ordType(0), 
		_price(0),
		_lastShares(0),
		_lastPrice(0),
		_leavesQty(0),
		_cumQty(0),
		_avgPrice(0),
        _timeInForce(0),
        _transactTime(),
        _transactTimeStr(""),
        _execInstStr(""),
        _handlInst(0),
        _ordRejReason(-1),
        _minQty(0),
        _exec_restatement_reason(-1),
        _exec_ref_id({0}),
        _state(OrderState_t::INT_STATE_UNINIT)

{ 

};


Order::Order(const Order& o) 
{
    pan::log_DEBUG("COPY CTOR Order(const Order& rhs)");
    _assign(o);
};

Order& 
Order::operator=(const Order& rhs) 
{
    if (&rhs == this) {
        return *this;
    }
    _assign(rhs);
    pan::log_DEBUG("ASSGN OPR Order& operator=(const Order& rhs)");
    return *this;

};

/*
bool
Order::operator==(const Order& rhs) const 
{
    return (this == &rhs || this->_clOid == rhs.getOid());
}
*/

Order::~Order() 
{ 

};	


void
Order::set(const capkproto::execution_report& er) 
{
    _clOid.set(er.cl_order_id().c_str(), er.cl_order_id().size());
    if (er.has_orig_cl_order_id()) {
        _origClOid.set(er.orig_cl_order_id().c_str(), er.orig_cl_order_id().size());
    }
    memcpy(_execId, er.exec_id().c_str(), er.exec_id().size());
    _execTransType = static_cast<capk::ExecTransType_t>(er.exec_trans_type());
    _ordStatus = static_cast<capk::OrdStatus_t>(er.order_status());
    _execType = static_cast<capk::ExecType_t>(er.exec_type());
    memcpy(_symbol, er.symbol().c_str(), er.symbol().size()); 
    std::string security_type = er.security_type();
    // assert(security_type == capk::SEC_TYPE_FOR);
    capkproto::side_t side = er.side();
    if (side == capkproto::BID) {
        _side = BID;
    }
    else if (side == capkproto::ASK) {
        _side = ASK;
    }
    _orderQty = er.order_qty();
    _ordType = er.ord_type();
    _price = er.price();
    _lastShares = er.last_shares();
    _lastPrice = er.last_price();
    _leavesQty = er.leaves_qty();
    _cumQty = er.cum_qty();
    _avgPrice = er.avg_price();
    _timeInForce = er.time_in_force();
    _transactTimeStr = er.transact_time();
    _execInstStr = er.exec_inst();
    _handlInst = er.handl_inst();
    _ordRejReason = er.order_reject_reason();
    _minQty = er.min_qty();
    _venue_id = er.venue_id();
    memcpy(_account, er.account().c_str(), ACCOUNT_LEN-1);
    memcpy(_exec_ref_id, er.exec_ref_id().c_str(), EXEC_REF_ID_LEN-1);
    _exec_restatement_reason = er.exec_restatement_reason();

};

void
Order::set(const capkproto::new_order_single& nos)
{
    _clOid.set(nos.cl_order_id().c_str(), nos.cl_order_id().size());
    memcpy(_symbol, nos.symbol().c_str(), nos.symbol().size());
    capkproto::side_t side = nos.side();
    if (side == capkproto::BID) {
        _side = BID;
    }
    else if (side == capkproto::ASK) {
        _side = ASK;
    }
    _orderQty = nos.order_qty();
    _ordType = nos.ord_type();
    _price = nos.price();
    _timeInForce = nos.time_in_force();
    assert(nos.account().size() < ACCOUNT_LEN);
    memcpy(_account, nos.account().c_str(), nos.account().size());
    _venue_id = nos.venue_id();

}

void
Order::set(const capkproto::order_cancel_replace& ocr)
{
    _origClOid.set(ocr.orig_cl_order_id().c_str(), ocr.orig_cl_order_id().size());
    _clOid.set(ocr.cl_order_id().c_str(), ocr.cl_order_id().size());
    _orderQty = ocr.order_qty();
    _price = ocr.price();
    _transactTimeStr = ocr.transact_time();
    _ordType = ocr.ord_type();
    _timeInForce = ocr.time_in_force();
}


/*
 * Used after an order update is received - 
 * should only update fields that may change during and execution 
 */
void
Order::update(const Order& o)
{
    // Don't set the following fields when updating:
    // _clOid 
    // _origClOid
    // _venue_id
    // since they should not change when updating an order
    memcpy(this->_execId, o.getExecId(), EXEC_ID_LEN);
    this->_execTransType = o.getExecTransType();
    this->_ordStatus = o.getOrdStatus();
    this->_execType = o.getExecType();
    // _symbol
    // _secType
    // _account
    // _side
    this->_orderQty = o.getOrdQty();
    this->_ordType = o.getOrdType();
    this->_price = o.getPrice();
    this->_lastShares = o.getLastShares();
    this->_lastPrice = o.getLastPrice();
    this->_leavesQty = o.getLeavesQty();
    this->_cumQty = o.getCumQty();
    this->_avgPrice = o.getAvgPrice();
    // _timeInForce
    this->_transactTime = o.getTransactTime();
    this->_transactTimeStr = o.getTransactTimeStr();
    // _execInstStr
    //this->_handlInst = o.getHandlInst();
    this->_ordRejReason = o.getOrdRejectReason();
    this->_minQty = o.getMinQty();
    this->_exec_restatement_reason = o.getExecRestatementReason();
    memcpy(this->_exec_ref_id, o.getExecRefId(), EXEC_REF_ID_LEN);
    // _state
}

void
Order::_assign(const Order& o)
{
    this->_clOid = o.getClOid();
    this->_origClOid = o.getOrigClOid();
    this->_venue_id = o.getVenueId();
    memcpy(this->_execId, o.getExecId(), EXEC_ID_LEN);
    this->_execTransType = o.getExecTransType();
    this->_ordStatus = o.getOrdStatus();
    this->_execType = o.getExecType();
    memcpy(this->_symbol, o.getSymbol(), SYMBOL_LEN);
    memcpy(this->_secType, o.getSecType(), SEC_TYPE_LEN);
    this->_side = o.getSide();
    this->_orderQty = o.getOrdQty();
    this->_ordType = o.getOrdType();
    this->_price = o.getPrice();
    this->_lastShares = o.getLastShares();
    this->_lastPrice = o.getLastPrice();
    this->_leavesQty = o.getLeavesQty();
    this->_cumQty = o.getCumQty();
    this->_avgPrice = o.getAvgPrice();
    this->_timeInForce = o.getTimeInForce();
    this->_transactTime = o.getTransactTime();
    this->_transactTimeStr = o.getTransactTimeStr();
    this->_execInstStr = o.getExecInstStr();
    this->_handlInst = o.getHandlInst();
    this->_ordRejReason = o.getOrdRejectReason();
    this->_minQty = o.getMinQty();
    this->_exec_restatement_reason = o.getExecRestatementReason();
    memcpy(this->_exec_ref_id, o.getExecRefId(), EXEC_REF_ID_LEN);
}

std::ostream& operator << (std::ostream& out, const Order& o) {
    uuidbuf_t oidbuf;
    uuidbuf_t origoidbuf;
    o.getClOid().c_str(oidbuf);
    o.getOrigClOid().c_str(origoidbuf);
    out << "Order:\n"
        << "cl_order_id=" << oidbuf
        << " orig_cl_order_id=" << origoidbuf
        << " venue_id=" << o._venue_id 
        << " exec_trans_type=" << o._execTransType
        << " order_status=" << static_cast<int>(o._ordStatus)
        << " exec_type=" << o._execType
        << " symbol=" << o._symbol
        << " side=" << o._side
        << " qty=" << o._orderQty 
        << " order_type=" << static_cast<int>(o._ordType)
        << " price=" << o._price 
        << " last_shares=" << o._lastShares 
        << " last_price=" << o._lastPrice 
        << " leaves_qty=" << o._leavesQty 
        << " cum_qty=" << o._cumQty 
        << " avg_price=" << o._avgPrice
        << " TIF=" << static_cast<int>(o._timeInForce)
        /*
        << " transact_time(EXCH)=" << o._transactTimeStr 
        << " exec_inst=" << o._execInstStr
        << " handl_inst=" << o._handlInst
        */
        << " order_reject_reason=" << o._ordRejReason
        << " min_qty=" << o._minQty
        << " exec_restatement_reason=" << o._exec_restatement_reason;

    return out;
}

} // namespace capk

