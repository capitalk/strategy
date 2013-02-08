
CC=g++

PROTOC=protoc

ROOT=../../..
SYS_ROOT=$(ROOT)/capitalk

COMMON=$(SYS_ROOT)/common
UTILS=$(COMMON)/src/c++
UTILS_SRC=$(UTILS)/utils
PROTO=$(COMMON)/proto

PANTHEIOS_EXT=$(ROOT)/pantheios_be_zmq

BOOST=/usr/local/boost
STRATEGY_BASE=..
STRATEGY_BASE_SRC=../strategy_base-C++

INCLUDE= -I $(COMMON) \
		 -I $(UTILS) \
		 -I $(PANTHEIOS_ROOT)/include \
		 -I $(STLSOFT)/include \
		 -I $(STRATEGY_BASE)

LIBS=	-lboost_date_time \
		-lboost_program_options \
		-lboost_filesystem \
		-lzmq \
		-lprotobuf \
		-lrt \
		-lboost_thread \
		-lgtest \
		#-lquickfix


PANTHEIOS_LIBS= -L $(PANTHEIOS_ROOT)/lib \
        -l pantheios.1.core.gcc44.file64bit \
        -l pantheios.1.fe.N.gcc44.file64bit \
        -l pantheios.1.util.gcc44.file64bit \
		-l pantheios.1.be.N.gcc44.file64bit \
        -l pantheios.1.bec.file.gcc44.file64bit \
        -l pantheios.1.bec.fprintf.gcc44.file64bit \
        -l pantheios.1.util.gcc44.file64bit \
        -l pantheios.1.core.gcc44.file64bit 

CFLAGS=-Wall -std=c++0x -pthread -O3 -ULOG -DNDEBUG
#LDFLAGS=-static

ifeq ($(dbg),1)
        CFLAGS = -Wall -std=c++0x -pthread -DDEBUG -ggdb  -DLOG -pg
		LDFLAGS= -pg
endif

all: all_strategies 

all_strategies: protos market_test.strat 

include $(PROTO)/Makefile.inc

market_test.strat: order_manager.o \
				strategy_protocol.o \
				market_test.o \
				client_order_interface.o \
				order_mux.o \
				logging.o \
				jenkins_hash.o \
				time_utils.o \
				timing.o \
			   	market_data_mux.o \
				client_market_data_interface.o \
				order.o \
				pantheios_be_zmq.o \
				bbo_book_types.o \
				config_server.o \
				$(PROTOBUF_OBJS)  
	$(CC) $(LDFLAGS) strategy_protocol.o order_manager.o market_test.o jenkins_hash.o client_order_interface.o order_mux.o logging.o time_utils.o timing.o market_data_mux.o client_market_data_interface.o order.o pantheios_be_zmq.o config_server.o bbo_book_types.o $(PROTOBUF_OBJS) /usr/local/lib/libquickfix.a $(LIBS) $(PANTHEIOS_LIBS) -o $@ 

order_manager.o: $(STRATEGY_BASE_SRC)/order_manager.cpp 
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

strategy_protocol.o: $(STRATEGY_BASE_SRC)/strategy_protocol.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

market_test.o: market_test.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

client_order_interface.o: $(STRATEGY_BASE_SRC)/client_order_interface.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

order_mux.o: $(STRATEGY_BASE_SRC)/order_mux.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

logging.o: $(UTILS_SRC)/logging.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

jenkins_hash.o: $(UTILS_SRC)/jenkins_hash.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

time_utils.o: $(UTILS_SRC)/time_utils.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

timing.o: $(UTILS_SRC)/timing.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

market_data_mux.o: $(STRATEGY_BASE_SRC)/market_data_mux.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

client_market_data_interface.o: $(STRATEGY_BASE_SRC)/client_market_data_interface.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

order.o: $(STRATEGY_BASE_SRC)/order.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

pantheios_be_zmq.o: $(PANTHEIOS_EXT)/pantheios_be_zmq.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) -Dunix $<

bbo_book_types.o: $(UTILS_SRC)/bbo_book_types.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<

config_server.o: $(UTILS_SRC)/config_server.cpp
	$(CC) -c $(INCLUDE) $(CFLAGS) $<


# PROTOS
protos: 
	cd $(PROTO) && make

tests: protos market_test.strat 

clean :
	rm -f *.o market_test.strat  $(STRATEGY_BASE)/*.o

install :
	echo "No install available yet"


