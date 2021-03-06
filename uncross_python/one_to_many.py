#!/usr/bin/python
# -*- coding: utf-8 -*-
from UserString import MutableString


class OneToManyDict:

    """Like a dictionary, but every key maps to a 
     set of values and the values map back to their
     key. 
  """

    def __init__(self):
        self.key_to_values = {}
        self.value_to_key = {}

    def get_values(self, k):
        return self.key_to_values[k]

    def get_key(self, v):
        return self.value_to_key[v]

    def add(self, k, v):
        #import uuid
        print 'Adding:[', k, v, ']'
        value_set = self.key_to_values.get(k, set([]))
        value_set.add(v)
        self.key_to_values[k] = value_set
        self.value_to_key[v] = k

    def __setitem__(self, k, v):
        return self.add(k, v)

    def __getitem__(self, k):
        return self.key_to_values[k]

    def remove_key(self, k):

    # first delete all the reverse mappings
    # from the values back to the key

        for v in self.get_values(k):
            del self.value_to_key[v]

        del self.key_to_values[k]

    def remove_value(self, v):
        k = self.get_key(v)
        value_set = self.key_to_values[k]
        value_set.remove(v)

    # Comment next line to leave values in
    # the value_to_key array

        del self.value_to_key[v]

    def has_key(self, k):
        return k in self.key_to_values

    def __contains__(self, k):
        return self.has_key(k)

    def has_value(self, v):
        return v in self.value_to_key

    def dbg_string(self):
        dbg_string = MutableString()
        dbg_string += '''
KEY TO VALUE:
'''
        for (k, v) in self.key_to_values.items():
            dbg_string += '\t%s => %s\n' % (k, v)
        dbg_string += 'VALUE TO KEY:\n'
        for (v, k) in self.value_to_key.items():
            dbg_string += '\t%s => %s\n' % (v, k)

    # print self.__str__()

        return dbg_string

    def __str__(self):
        return 'k2v: %s, v2k: %s' % (self.key_to_values,
                self.value_to_key)


