# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 14:56:21 2015

@author: vpappakrishnan
"""
#./bin/pyspark --master local[8]
from pyspark import SparkContext, SparkConf
import os
os.environ['SPARK_HOME'] = "C:\Spark"

AppName = 'local'
master = 'My App'
conf = SparkConf().setMaster('local').setAppName(AppName)

sc = SparkContext(conf = conf)
lines = sc.textFile("C:\Spark\README.md")
print("No. of. lines:", lines.count())

lines_error = lines.filter(lambda x: "of" in x)
print("Lines with error:", lines_error.count())

sc.stop()