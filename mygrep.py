#!/usr/bin/env python2
from __future__ import print_function

import sys
from sys import argv
import re

expression, argv = argv[1], argv[2:]

print("searching for {0}".format(expression),file=sys.stderr)

numeric_const_pattern = r"""
     [-+]? # optional sign
     (?:
         (?: \d* \. \d+ ) # .1 .12 .123 etc 9.1 etc 98.1 etc
         |
         (?: \d+ \.? ) # 1. 12. 123. etc 1 12 123 etc
     )
     # followed by optional exponent part if desired
     (?: [Ee] [+-]? \d+ ) ?
     """

rx = re.compile(numeric_const_pattern, re.VERBOSE)

for path in argv:
    vals = []
    with open(path,'r') as f:
        for line in f:
            match = re.search(expression,line)
            if match:
                val = rx.findall(match.group(0))
                vals.append(float(val[-1]))
print(sum(vals)/len(vals))
