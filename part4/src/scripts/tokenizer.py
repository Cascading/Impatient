#!/usr/bin/env python
# encoding: utf-8

import re
import sys

pat_l = re.compile("\w.*")
pat_r = re.compile(".*\w")

def tokenize (line):
    """
    split a line of text into a stream of tokens, while scrubbing the tokens
    """

    for token in map(lambda t1: re.search(pat_r, t1).group(), map(lambda t0: re.search(pat_l, t0).group(), line.split(" "))):
        if len(token) > 0:
            yield token


if __name__ == "__main__":
    for line in sys.stdin:
        for token in tokenize(line.strip().lower()):
            print token
