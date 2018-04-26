#!/usr/bin/env python
  
import os
import io
import sys

stopword = [":",",",""," ","[","]","\n","{","}"]

filename = os.environ.get("mapreduce_map_input_file")
for i in sys.stdin:
    if i.startswith('  "data"'):
        for j in i.split(",")[1:]:
            j=j.strip(''.join(stopword))
            if j not in stopword:
                print('{0:s}\t{1:s},{2:d}'.format(j,filename,1))
    elif i.startswith(', [ '):
        for j in i.split(","):
            j=j.strip(''.join(stopword))
            if j not in stopword:
                print('{0:s}\t{1:s},{2:d}'.format(j,filename,1))
