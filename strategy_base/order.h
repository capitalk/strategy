#ifndef __ORDER_H__
#define __ORDER_H__

#include "proto/execution_report.pb.h"
#include "proto/new_order_single.pb.h"
#include "proto/order_cancel.pb.h"
#include "proto/order_cancel_replace.pb.h"

#include "utils/logging.h"
#include "utils/types.h"
#include "utils/constants.h"
#include "utils/order_constants.h"
#include "utils/fix_convertors.h"
// These definitions are based on fix 4.2 dictionary from fixprotocol.org
// but should be pretty universal since we're using a relatively small subset
// of the FIX protocol

namespace capk {

class Order;
typedef boost::shared_ptr<Order> Order_ptr_t;

class Order 
{
	public: 
/* 
        Order() : _oid(false) { };
		Order(order_id_t& oid): _oid(oid) { };
		Order(const Order& o) {
            std::cerr << "COPY CTOR Order(const Order& rhs)" << std::endl;
        };
        Order& operator=(const Order& rhs) {
            if (&rhs == this) {
                return *this;
            }
            std::cerr << "ASSGN OPR Order& operator=(const Order& rhs)" << std::endl;
            return *this;
           
        };
		~Order() { };	
*/
        Order();
		Order(order_id_t& oid);
		Order(const Order& o);
        Order& operator=(const Order& rhs);
		~Order(); 	

        void set(const capkproto::execution_report& er);
        void set(const capkproto::new_order_single& er);

        void update(const Order& order);

        order_id_t getOid() const { return _oid;}
        order_id_t getOrigClOid() const  { return _origClOid;}
        const char* getExecId() const { return _execId;}
        ExecTransType_t getExecTransType() const { return _execTransType;}
        OrdStatus_t getOrdStatus() const { return _ordStatus;}
        ExecType_t getExecType() const { return _execType;}
        const char* getSymbol() const { return _symbol;}
        const char* getSecType() const { return _secType;}
        Side_t getSide() const { return _side;}
        double getOrdQty() const { return _orderQty;}
        OrdType_t getOrdType() const { return _ordType;}
        double getPrice() const { return _price;}
        double getLastShares() const { return _lastShares;}
        double getLastPrice() const { return _lastPrice;}
        double getLeavesQty() const { return _leavesQty;}
        double getCumQty() const { return _cumQty;}
        double getAvgPrice() const { return _avgPrice;}
        TimeInForce_t getTimeInForce() const { return _timeInForce;}
        timespec getTransactTime() const { return _transactTime;}
        std::string getTransactTimeStr() const { return _transactTimeStr;}
        std::string getExecInstStr() const { return _execInstStr;}
        HandlInst_t getHandlInst() const { return _handlInst;}
        OrdRejectReason_t getOrdRejectReason() const { return _ordRejReason;}
        double getMinQty() const { return _minQty;}

        void setOid(const order_id_t& oid) { this->_oid = oid;}

        friend std::ostream& operator << (std::ostream& out, const Order& o);

	private: 
        void assign(const capk::Order&);

		order_id_t _oid;
        venue_id_t _venue_id;
		order_id_t _origClOid;
		char _execId[EXEC_ID_LEN];	
		ExecTransType_t _execTransType;
		OrdStatus_t _ordStatus;
		ExecType_t _execType;
		char _symbol[SYMBOL_LEN];
		char _secType[SEC_TYPE_LEN];
        char _account[ACCOUNT_LEN];
		Side_t _side;
		double _orderQty;
		OrdType_t _ordType;
		double _price;
		double _lastShares;
		double _lastPrice;
		double _leavesQty;
		double _cumQty;
		double _avgPrice;
		TimeInForce_t _timeInForce;
		timespec _transactTime;
        std::string _transactTimeStr;
        std::string _execInstStr;
		HandlInst_t _handlInst;
		OrdRejectReason_t _ordRejReason;
        double _minQty;
        ExecRestatementReason_t _exec_restatement_reason;
        char _exec_ref_id[EXEC_REF_ID_LEN];
		

};




}; // namespace capk
#endif // __ORDER_H__

