import sys
import one_to_many
from one_to_many import OneToManyDict

m = OneToManyDict()

def create_map():
    m.add('a', 1)
    m.add('a', 2)
    m.add('a', 3)
    m.add('b', 4)
    m.add('c', 5)
    print m.dbg_string()

if __name__ == '__main__':
    create_map()

    print "remove_value(2)"
    m.remove_value(2)
    print m.dbg_string()

    print "remove_key(b)"
    m.remove_key('b')
    print m.dbg_string()

    print "remove_key(c)"
    m.remove_key('c')
    print m.dbg_string()
    
    print "remove_value(3)"
    m.remove_value(3)
    print m.dbg_string()

    print "remove_value(1)"
    m.remove_value(1)
    print m.dbg_string()

    print "remove_key(a)"
    m.remove_key('a')
    print m.dbg_string()
