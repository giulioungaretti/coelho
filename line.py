#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  2015 giulio <giulioungaretti@me.com>
"""
Small helper to write to std out  bytestream formatted as follows:
    routingKey message
"""
import json
import sys


def send(line):
    js = json.loads(line)
    s = js.get("scope", None)
    a = js.get("action", None)
    rk = "{}.{}".format(s, a)
    sys.stdout.write('"{}" "{}"'.format(rk,  line))

if __name__ == '__main__':
    for line in sys.stdin:
        send(line)
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4
# vim: filetype=python foldmethod=indent
