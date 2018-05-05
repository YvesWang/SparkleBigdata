#!/usr/bin/env python
  
import os
import io
import sys
current_key = None
for line in sys.stdin:
    line = line.strip()
    if current_key == line:
        continue
    else:
        if(current_key):
            print(current_key)
        current_key = line
if(current_key):
    print(current_key)

