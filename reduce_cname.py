#!/usr/bin/env python
  
import sys
import string

lastkey = 'start'

file_count_list = []
filecount = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    
    if lastkey == 'start':
        lastkey = key

    if key == lastkey:
        file_count_list.append('{0:s}'.format(value))

    else:
        print('{0:s}\t{1:s}'.format(lastkey,";".join(file_count_list)))
        lastkey = key
        file_count_list = ['{0:s}'.format(value)]
        
print('{0:s}\t{1:s}'.format(lastkey,";".join(file_count_list)))
