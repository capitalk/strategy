#ifndef __POSITION_H__
#define __POSITION_H__

#include "utils/logging.h"
#include "utils/types.h"
#include "utils/constants.h"
#include "utils/order_constants.h"
#include "utils/fix_convertors.h"

#include <string>
#include <ostream>

namespace capk {

class Position 
{
	public: 
        Position(const char* symbol, 
                double init_long_pos = 0, 
                double init_short_pos = 0, 
                double init_long_val = 0, 
                double init_short_val = 0):
            _symbol(symbol),
            _long_pos(init_long_pos),
            _short_pos(init_short_pos), 
            _long_value(init_long_val),
            _short_value(init_short_val)
        {
        }

		Position(const Position& p);

        Position& operator=(const Position& rhs);

		~Position(); 	

        const std::string& symbol() const { return _symbol;}
        double long_pos() const { return _long_pos;}
        double short_pos() const { return _short_pos;}
        double long_value() const { return _long_value;}
        double short_value() const { return _short_value;}

        // Computed values - never stored
        double net_pos() const { return _long_pos - _short_pos;}
        double long_avg_price() const { long_pos() > 0 ? long_value() / long_pos() : 0.0; }
        double short_avg_price() const { short_pos() > 0 ? short_value() / short_pos() : 0.0; }

        friend std::ostream& operator << (std::ostream& out, const Position& p);

    private: 
        std::string _symbol;
		double _long_pos;
		double _short_pos;
		double _long_value;
		double _short_value;
};

std::ostream& operator << (std::ostream& out, const Position& p) {
    out << "Position <"
        << "symbol=" << p._symbol 
        << "long_pos=" << p._long_pos 
        << "short_pos=" << p._short_pos 
        << "long_val=" << p._long_value
        << "short_val=" << p._short_value
        << "net_pos=" << p.net_pos() 
        << "long_avg_price=" << p.long_avg_price()
        << "short_avg_price=" << p.short_avg_price();
    return out;
}


}; // namespace capk



#endif // __POSITION_H__

