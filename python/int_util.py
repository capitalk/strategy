#!/usr/bin/python
# -*- coding: utf-8 -*-

import struct


def int_from_bytes(bytes):
    assert len(bytes) == 4
    return struct.unpack('<I', bytes)[0]


def int_to_bytes(i):
    return struct.pack('<I', i)


