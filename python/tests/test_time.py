import time
import sys


t0 = time.time()
time.sleep(10)
t1 = time.time() - t0
print "Elapsed time: ", t1
rescue_expired = time.time() - t0 >= 10

if rescue_expired:
    print "Expired"
else:
    print "Not expired"
