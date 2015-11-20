# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 10:59:16 2015

@author: vpappakrishnan
"""

from pyspark import SparkContext, SparkConf
import os
os.environ['SPARK_HOME'] = "C:\Spark"

def stat(data):
    return 'of' in data

AppName = 'hello'
master = 'local'
conf = SparkConf().setMaster(master).setAppName(AppName)

sc = SparkContext(conf = conf)

rdd = sc.textFile("C:\Spark\README.md")
lines_count = rdd.filter(stat).cache()

# Just prints the number of lines with 'of'
print(lines_count.count())

# prints the rdd
print(lines_count.collect())


sc.stop()