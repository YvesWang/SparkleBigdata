#!/usr/bin/env python
from csv import reader
import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("Test")
sc = SparkContext(conf=conf)
def search(query):
	text = sc.textFile("/user/ys3225/1004project/result0.out",200)
	result = text.filter(lambda x: x.startswith(query)).filter(lambda x: x.split('\t|:|;')[])
