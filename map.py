#!/usr/bin/env python
  
import os
import io
import sys
import csv
stopword = [":",",",""," ","[","]","\n","{","}"]

filename = os.environ.get("mapreduce_map_input_file")
for line in sys.stdin:
    if line.startswith('  "data"'):
        for j in csv.reader(line.strip()):
            j=str(j[0]).strip(''.join(stopword)).lower()
            if j not in stopword:
                print('{0:s}\t{1:s}'.format(j,filename))
    elif line.startswith(', [ '):
        for j in csv.reader(line.strip()):
            j=str(j[0]).strip(''.join(stopword)).lower()
            if j not in stopword:
                print('{0:s}\t{1:s}'.format(j,filename))
