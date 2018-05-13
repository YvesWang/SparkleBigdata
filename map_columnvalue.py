#!/usr/bin/env python
  
import os
import io
import sys
import csv
import re
stopword = [",",""," ","[","]","\n","{","}",";","#","!","@","^","&"]
csv.register_dialect('myreader',skipinitialspace = True, escapechar = '\\')
filename = os.environ.get("mapreduce_map_input_file")
try:
    for line in csv.reader(sys.stdin,'myreader'):
        if len(line)<3:
            continue
        if line[0].startswith('data : [ ['):
            first = line[0].split(' ')[-1].strip(''.join(stopword)).lower()
            #if first not in stopword:
            if first not in stopword and re.match(r'(?!^[0-9]*$)(^[0-9a-zA-Z ]+$)',first):
            #if first not in stopword and re.match(r'^[0-9]*$',first):
                print('{0:s},,,,{1:d}\t{2:s}'.format(filename,0,first))
            for j in range(len(line[1:])):
                word = line[1:][j]
                word = word.strip(''.join(stopword)).lower()
                #if word not in stopword:
                if word not in stopword and re.match(r'(?!^[0-9]*$)(^[0-9a-zA-Z ]+$)',word):
                #if word not in stopword and re.match(r'^[0-9]*$',word):
                    print('{0:s},,,,{1:d}\t{2:s}'.format(filename,j+1,word))
        elif line[0]== '' and line[1].startswith('[ '):
            first = line[1].split(' ')[-1].strip(''.join(stopword)).lower()
            #if first not in stopword:
            if first not in stopword and re.match(r'(?!^[0-9]*$)(^[0-9a-zA-Z ]+$)',first):
            #if first not in stopword and re.match(r'^[0-9]*$',first):
                print('{0:s},,,,{1:d}\t{2:s}'.format(filename,0,first))
            for j in range(len(line[2:])):
                word = line[2:][j]
                word = word.strip(''.join(stopword)).lower()
                #if word not in stopword:
                if word not in stopword and re.match(r'(?!^[0-9]*$)(^[0-9a-zA-Z ]+$)',word):
                #if word not in stopword and re.match(r'^[0-9]*$',word):
                    print('{0:s},,,,{1:d}\t{2:s}'.format(filename,j+1,word))
except:
    None
