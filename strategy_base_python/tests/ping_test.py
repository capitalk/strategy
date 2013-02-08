#!/usr/bin/python
import sys
import zmq
import uuid
import time
import struct

sid = "14a120c0-1fb7-411d-b7ff-a327daa20c61" 
context = zmq.Context()




def int_from_bytes(bytes):
    assert len(bytes) == 4
    return struct.unpack('<I', bytes)[0]


def int_to_bytes(i):
    return struct.pack('<I', i)

def ping(socket, name=None):
    t0 = time.time()
    socket.send(int_to_bytes(0xF2))


if __name__== '__main__':
    hello_tag = int_to_bytes(0xF0)

    # None of these operations will block, regardless of peer:
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    #socket.connect("tcp://127.0.0.1:7997")
    x = socket.connect("tcp://127.0.0.1:7999")
    print "---------------> ", x
    print time.time()    
    ping(socket)    

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    if poller.poll(3*1000): # 10s timeout in milliseconds
        msg = socket.recv()
        print time.time()    
        print int_from_bytes(msg)
    else:
        print time.time()    
        raise IOError("Timeout processing auth request")

    # these are not necessary, but still good practice:
    socket.close()
    context.term()
    sys.exit(0)


