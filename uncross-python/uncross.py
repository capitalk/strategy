import gevent 
import zmq
import proto_objs.spot_fx_md_1_pb2
import time 

root_thread = gevent.getcurrent()

def read_symbols(filename):
  f = open(filename)
  syms = [] 
  for line in f.readlines():
    line = line.strip()
    if len(line) == 0: continue
    elif len(line) != 7 or line[3] != "/": 
      raise RuntimeError("Invalid ccy format " + line)
    else:
      syms.append( line)
  f.close()
  print syms 
  return syms

context = zmq.Context()

def make_subscriber(addr = "tcp://127.0.0.1", port = 5273, symbols = None):
  
  socket = context.socket(zmq.SUB)
  target = "%s:%s" % (addr, port)
  print "Making socket for", target
  socket.connect(target)
  if symbols is None:
    print "Subscribing to all messages" 
    socket.setsockopt(zmq.SUBSCRIBE, "")
  else:
    for sym in symbols:
      print "Subscribing to", sym 
      socket.setsockopt(zmq.SUBSCRIBE, sym)
  return socket 


orderbook = {} 
# REFRACTORY PERIOD FOR PLACING ORDERS wait_until = None 

def receive_market_data(md_addr, md_port, order_addr, order_port, symbols):
  md_socket = make_subscriber(md_addr, md_port, symbols) 
  while True:
    msg_parts = md_socket.recv_multipart()
    if len(msg_parts) != 2:
      print "Received unrecognized message", msg_parts
    else: 
      [topic, contents] = msg_parts
      bbo = proto_objs.spot_fx_md_1_pb2.instrument_bbo();
      bbo.ParseFromString(contents);
      print bbo.symbol, bbo.bb_venue_id, bbo.bb_price, bbo.bb_size, "@", bbo.ba_venue_id, bbo.ba_price, bbo.ba_size
    root_thread.sleep(0)

def receive_order_updates(order_addr, order_port):
  socket = make_subscriber(order_addr, order_port)
  while True:
    msg_parts = socket.recv_multipart()
    print msg_parts
    root_thread.sleep(0)



from argparse import ArgumentParser 
parser = ArgumentParser(description='Market uncrosser') 
parser.add_argument('--market-data-addr', type=str, default='tcp://127.0.0.1', dest = 'md_addr')
parser.add_argument('--market-data-port', type=int,  required=True, dest='md_port')
parser.add_argument('--order-engine-addr', type=str, default='tcp://127.0.0.1', dest='order_addr')
parser.add_argument('--order-engine-port', type=int, required=True, dest='order_port')
parser.add_argument('-s', '--symbols-file', type=str, default=None, dest='symbols_file')

if __name__ == '__main__':
  args = parser.parse_args()
  symbols = read_symbols(args.symbols_file) if args.symbols_file else None
  
  md_thread = gevent.spawn(receive_market_data, args.md_addr, args.md_port, args.order_addr, args.order_port, symbols)
  order_thread = gevent.spawn(receive_order_updates, args.order_addr, args.order_port)
  gevent.joinall([md_thread, order_thread])


