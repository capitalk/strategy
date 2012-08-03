
CC=g++

PROTOC=protoc


SRC=..

BOOST=/usr/local/boost
COLLECTORS=../../collectors
ORDER_ENGINES=../../order_engines/order-fix
STRATEGY_BASE=../strategy_base
UTILS=$(COLLECTORS)/utils
PROTO=$(COLLECTORS)/proto


INCLUDE=-I$(BOOST) -I/usr/local/include/ \
		-I..\
	   	-I$(COLLECTORS) \
		-I $(PANTHEIOS_ROOT)/include \
		-I $(STLSOFT)/include \
		-I $(UTILS) \
		-I $(ORDER_ENGINES) \
		-I $(STRATEGY_BASE)

LIBS= -L/usr/local/src/quickfix/lib/ \
		-L/usr/local/lib \
      	-lquickfix -lboost_date_time \
		-lboost_program_options \
		-lboost_filesystem \
		-lzmq \
		-lprotobuf \
		-lrt \
		-lboost_thread \
		-lgtest


PANTHEIOS_LIBS= -L $(PANTHEIOS_ROOT)/lib \
        -l pantheios.1.core.gcc44.file64bit \
        -l pantheios.1.fe.simple.gcc44.file64bit.debug \
        -l pantheios.1.util.gcc44.file64bit \
		-l pantheios.1.be.N.gcc44.file64bit \
        -l pantheios.1.bec.file.gcc44.file64bit \
        -l pantheios.1.bec.fprintf.gcc44.file64bit \
        -l pantheios.1.util.gcc44.file64bit \
        -l pantheios.1.core.gcc44.file64bit 

CFLAGS=-Wall -std=c++0x -pthread -O3 -ULOG -DNDEBUG
LDFLAGS=-static

ifeq ($(dbg),1)
        CFLAGS = -Wall -std=c++0x -pthread -DDEBUG -ggdb  -DLOG -pg
		LDFLAGS= -pg
endif

all: all_strategies 

all_strategies: protos test_strategy.strat

include $(PROTO)/Makefile.inc

test_strategy.strat: test_strategy.o \
				order_manager.o \
				strategy_protocol.o \
				client_order_interface.o \
				order_mux.o \
				logging.o \
				jenkins_hash.o \
				time_utils.o \
				timing.o \
			   	market_data_mux.o \
				client_market_data_interface.o \
				order.o \
				$(PROTOBUF_OBJS)  
	$(CC) $(LDFLAGS) test_strategy.o order_manager.o strategy_protocol.o jenkins_hash.o client_order_interface.o order_mux.o logging.o time_utils.o timing.o market_data_mux.o client_market_data_interface.o order.o $(PROTOBUF_OBJS) $(LIBS) $(PANTHEIOS_LIBS) -o $@ 

test_strategy.o: test_strategy.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

strategy_protocol.o: $(STRATEGY_BASE)/strategy_protocol.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

order_manager.o: $(STRATEGY_BASE)/order_manager.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

client_order_interface.o: $(STRATEGY_BASE)/client_order_interface.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

order_mux.o: $(STRATEGY_BASE)/order_mux.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

market_data_mux.o: $(STRATEGY_BASE)/market_data_mux.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

client_market_data_interface.o: $(STRATEGY_BASE)/client_market_data_interface.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

order.o: $(STRATEGY_BASE)/order.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

logging.o: $(UTILS)/logging.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

jenkins_hash.o: $(UTILS)/jenkins_hash.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

time_utils.o: $(UTILS)/time_utils.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

timing.o: $(UTILS)/timing.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<


# PROTOS
protos: 
	cd $(PROTO) && make

tests: protos test_strategy.strat 

clean :
	rm -f *.o test_strategy.strat  $(STRATEGY_BASE)/*.o

install :
	echo "No install available yet"


