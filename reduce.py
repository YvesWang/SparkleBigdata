#!/usr/bin/env python
  
import sys
import string

lastkey = 'start'
lasttruekey = 'start'
lastfilekey = 'start'

file_count_list = []
filecount = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    #filename, count = value.split(',')
    truekey, filekey = key.split(',')
    value = int(value)

    if lasttruekey == 'start':
        lasttruekey = truekey
        lastfilekey = filekey

    if truekey == lasttruekey:
        if filekey == lastfilekey:
            filecount += value
        else:
            file_count_list.append('{0:s}:{1:d}'.format(lastfilekey,filecount))
            lastfilekey = filekey
            filecount = value

    else:
        file_count_list.append('{0:s}:{1:d}'.format(lastfilekey,filecount))
        print('{0:s}\t{1:s}'.format(lasttruekey,";".join(file_count_list)))
        lasttruekey = truekey
        file_count_list = []
        filecount = value
        lastfilekey = filekey
        
file_count_list.append('{0:s}:{1:d}'.format(lastfilekey,filecount))
print('{0:s}\t{1:s}'.format(lasttruekey,";".join(file_count_list)))
