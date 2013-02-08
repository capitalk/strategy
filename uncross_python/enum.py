#!/usr/bin/python
# -*- coding: utf-8 -*-


class enum:

    def __init__(self, *just_names, **variants):
        self.value_to_name = {}
        for (i, name) in enumerate(just_names):
            self.value_to_name[i] = name
            setattr(self, name, i)

        for (name, value) in variants.items():
            self.value_to_name[value] = name
            setattr(self, name, value)

    def map(self, fn):
        other = enum()
        for (i, name) in self.value_to_name.iteritems():
            j = fn(i)
            other.value_to_name[j] = name
            setattr(other, name, j)
        return other

    def to_str(self, value):
        if value in self.value_to_name:
            return self.value_to_name[value]
        if value is None:
            return 'NONE'
        else:
            raise RuntimeError('No variant found in code %s' % value)


