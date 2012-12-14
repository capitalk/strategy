import zmq
import proto_objs.spot_fx_md_1_pb2

# Create context and connect
context = zmq.Context()
socket = context.socket(zmq.SUB)

# Use below for direct market subscription

#socket.connect("tcp://127.0.0.1:5997")
# Use below for aggregated book
socket.connect("tcp://localhost:10000")

socket.setsockopt(zmq.SUBSCRIBE, "")
# Use below for direct market subscription
#socket.setsockopt(zmq.SUBSCRIBE, "EUR/USD")

while True:
    # N.B. only aggregated book has topc and contents
    # while direct market data sends only protobuf
    [topic, contents] = socket.recv_multipart()
    #contents = socket.recv()
    print topic
    print contents
    
    #bbo = proto_objs.spot_fx_md_1_pb2.instrument_bbo();
    #bbo.ParseFromString(contents);
    #print bbo.symbol, bbo.bb_venue_id, bbo.bb_price, bbo.bb_size, "@", bbo.ba_venue_id, bbo.ba_price, bbo.ba_size

