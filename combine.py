#!/usr/bin/env python
  
import os
import io
import sys
total_count = 0
current_key = None
for line in sys.stdin:
    line = line.strip()
    key, filename = line.split('\t')
    count = 1
    if current_key == key:
        total_count += count
    else:
        if(current_key):
            print('{0:s};;{1:s}\t{2:d}'.format(current_key,filename,total_count))
        total_count = count
        current_key = key
if(current_key):
    print('{0:s};;{1:s}\t{2:d}'.format(current_key,filename,total_count))

