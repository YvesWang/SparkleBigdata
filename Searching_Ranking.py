#!/usr/bin/env python
from csv import reader
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
import re

conf = SparkConf().setMaster("local").setAppName("Test")

text = sc.textFile("/user/tw1682/Bigdata/Project/result0",200)
textrdd = text.map(lambda x: Row(Key=x.split('\t')[0],Filename_Count=x.split('\t')[1]))
textframe = spark.createDataFrame(textrdd)

def search_startwith(textframe):
	return textframe.filter(textframe['Key'].startswith(query))


def search_accuracy(query):
	return textframe.filter(textframe['Key'] == query)


def search_contain(query):
	return textframe.filter(textframe['Key'].like('%'+query+'%'))


def search_sim(filecolumn,value):
    ColSig = sc.textFile('/user/tw1682/Bigdata/Project/ColSignature/ColSig')
    querySig = ColSig.filter(lambda x:x.split('\t')[0] == filecolumn).map(lambda x: x.split('\t')[1].split(',')).collect()
    SigList = ColSig.map(lambda x:(sum([x.split('\t')[1].split(',')[i] == querySig[0][i] for i in range(6)])/(12-sum([x.split('\t')[1].split(',')[i] == querySig[0][i] for i in range(6)])),x.split('\t')[0])).sortByKey(ascending= False)
    return SigList

def search(querys):
    flag = True
    for query in querys:
        query = query.lower()
        if re.match(r'^\*.+\*$',query):
            query = query[1:-1]
            #textframe = textframe.filter(textframe['Key'].like('%'+query+'%'))
            if re.match(r'^\d+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIdigit.out",200)
            elif re.match(r'^[a-zA-Z ]+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/alphabet",200)
            else:
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIALL",200)
            in388 = IndexDic.filter(lambda x: re.match(r'^.*'+query+'.*\t.*$',x))
        elif re.match(r".+\*$",query):
            query = query[:-1]
            #textframe=textframe.filter(textframe['Key'].startswith(query))
            if re.match(r'^\d+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIdigit.out",200)
            elif re.match(r'^[a-zA-Z ]+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/alphabet_"+query[0],200)
            else:
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIALL",200)
            in388 = IndexDic.filter(lambda x: re.match(r'^'+query+'.*\t.*$',x)) 
        else: #call_accuracy
            if re.match(r'^\d+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIdigit.out",200)
            elif re.match(r'^[a-zA-Z ]+$',query):
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/alphabet_"+query[0],200)
            else:
                IndexDic = sc.textFile("Bigdata/Project/HashIndex/IIALL",200)
            in388 = IndexDic.filter(lambda x: re.match(r'^'+query+'\t.*$',x))    
        if flag:
            result = in388.intersection(in388)
            flag = False
            continue
        result = result.intersection(in388)
    Blurkey = result.map(lambda x: x.split('\t')[0])
    insplit = result.map(lambda x: (x.split('\t')[0],x.split('\t')[1])).flatMapValues(lambda x:x.split(';;;;')).map(lambda x: (x[0],x[1].split('::::')[0],x[1].split('::::')[1])).sortBy(lambda x: -int(x[2]))
    return insplit
