#!/usr/bin/env python
  
import os
import io
import sys
import csv
stopword = [":",",",""," ","[","]","\n","{","}"]

#filename = os.environ.get("mapreduce_map_input_file")
filename = sys.argv[1]
for line in sys.stdin:
    if line.startswith('        "name"'):
        j=line.split(":")[-1].strip(''.join(stopword))
        if j not in stopword:
            print('{0:s}\t{1:s}'.format(j,filename))
