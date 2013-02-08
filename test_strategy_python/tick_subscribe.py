import zmq
import proto_objs.spot_fx_md_1_pb2
import proto_objs.venue_configuration_pb2
import strategy_base_python.configuration_server
from strategy_base_python.configuration_server import Configuration

# Create context and connect
context = zmq.Context()
socket = context.socket(zmq.SUB)

# Use below for direct market subscription
# Use below for aggregated book
#socket.connect("tcp://*:10000")

# Use below for aggregated book
socket.setsockopt(zmq.SUBSCRIBE, "")
# Use below for direct market subscription
#socket.setsockopt(zmq.SUBSCRIBE, "EUR/USD")
socket.setsockopt(zmq.LINGER, 0)

def recv_direct(socket):
    while True:
        # NB - single message - no wrapper
        contents = socket.recv()
        bbo = proto_objs.spot_fx_md_1_pb2.instrument_bbo();
        bbo.ParseFromString(contents)
        print bbo.__str__()

def recv_aggregated(socket):
    while True:
        [topic, contents] = socket.recv_multipart()
    #print topic
    #print contents

        bbo = proto_objs.spot_fx_md_1_pb2.instrument_bbo();
        bbo.ParseFromString(contents)
        print bbo.__str__()
        #print bbo.symbol, bbo.bid_venue_id, bbo.bid_price, bbo.bid_size, "@", bbo.ask_venue_id, bbo.ask_price, bbo.ask_size

    
def run(direct, connect_addr): 
    socket.connect(connect_addr)
    socket.setsockopt(zmq.SUBSCRIBE, "")
    socket.setsockopt(zmq.LINGER, 0)
    if direct == True:
        print "Connecting direct"
        recv_direct(socket);
    else:
        print "Connecting aggregated"
        recv_aggregated(socket);


from argparse import ArgumentParser
if __name__ == '__main__':
    parser = ArgumentParser(description='Tick Subscribe', add_help=True)
    parser.add_argument('--config-server', type=str, default='tcp://*:11111', 
            dest='config_server', help='address of configuration server')

    parser.add_argument('-d', '--direct', action='store_true',
            dest='direct_market_subscription', 
            help='listen direct to given market')
    
    parser.add_argument('-a', '--aggregated', action='store_true',
            dest='aggregated_market_subscription',
            help='listen to aggregated book feed')

    parser.add_argument('-v', '--venue', 
            dest='venue_id',
            help='listen to specified venue_id only')

    args = parser.parse_args()

    if args.aggregated_market_subscription is False and args.venue_id is None:
        print "\nYou must choose either aggregated book or supply a venue id\n"
        parser.print_help();
        exit(-1)

    config = Configuration(context)
    config.connect(args.config_server)

    global_config = config.get_configs();
    venue_configs = global_config.configs

    selected_config = proto_objs.venue_configuration_pb2.venue_configuration()

    for venue_config in venue_configs:
        venue_id = str(venue_config.venue_id)
        mic_name = str(venue_config.mic_name)
        if venue_id == str(args.venue_id) or mic_name == str(args.venue_id):
            selected_config = venue_config 


    if (args.direct_market_subscription): 
        run(direct=True, connect_addr=selected_config.market_data_broadcast_addr)
        if args.venue_id is None:
            parser.print_help()
            exit(-1)
    elif (args.aggregated_market_subscription):
        print "Subscribing aggregated"
        run(direct=False, connect_addr=global_config.aggregated_bbo_book_addr)
    else:
        print parser.print_help();
        exit



