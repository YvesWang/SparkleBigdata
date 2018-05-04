#!/usr/bin/env python
  
import os
import io
import sys
import csv
stopword = [",",""," ","[","]","\n","{","}",";","#","!","@","^","&"]
csv.register_dialect('myreader',skipinitialspace = True, escapechar = '\\')
#print(csv.list_dialects())
#print('******')
#o = csv.get_dialect('myreader')
#quoting_modes = dict( (getattr(csv,n), n) for n in dir(csv) if n.startswith('QUOTE_') )
#print(o.delimiter, o.skipinitialspace, o.doublequote,o.quotechar,o.lineterminator, quoting_modes[o.quoting],o.escapechar)
#print('{0:s};{1:d};{2:d};{3:s};{4:s};{5:s};{6:s}'.format(o.delimiter, o.skipinitialspace, o.doublequote,o.quotechar,o.lineterminator, quoting_modes[o.quoting],o.escapechar))
filename = os.environ.get("mapreduce_map_input_file")

for line in csv.reader(sys.stdin,'myreader'):
    if len(line)<3:
        continue
    if line[0].startswith('data : [ ['):
        first = line[0].split(' ')[-1].strip(''.join(stopword)).lower()
        if first not in stopword:
            print('{0:s}\t{1:s}'.format(first,filename))
        for j in line[1:]:
            #j=str(j[0]).strip(''.join(stopword)).lower()
            j=j.strip(''.join(stopword)).lower()
            if j not in stopword:
                print('{0:s}\t{1:s}'.format(j,filename))
    elif line[0]== '' and line[1].startswith('[ '):
        first = line[1].split(' ')[-1].strip(''.join(stopword)).lower()
        if first not in stopword:
            print('{0:s}\t{1:s}'.format(first,filename))
        for j in line[2:]:
            #j=str(j[0]).strip(''.join(stopword)).lower()
            j=j.strip(''.join(stopword)).lower()
            if j not in stopword:
                print('{0:s}\t{1:s}'.format(j,filename))
