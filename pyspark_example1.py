# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 14:56:21 2015

@author: vpappakrishnan
"""
#./bin/pyspark --master local[8]
from pyspark import SparkContext, SparkConf
import os
import sys

os.environ['SPARK_HOME'] = "C:\Spark"
#print(os.environ.get('SPARK_HOME'))

# Path to Hadoop that directs to winutils.exe
os.environ['HADOOP_HOME'] = "C:\Hadoop\bin"
os.environ['JAVA_HOME'] = 'C:\Program Files (x86)\Java\jre1.8.0_60'
os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local pyspark-shell"

sys.path.append("C:\Spark\python")
sys.path.append("C:\Spark\python\lib\py4j-0.8.2.1-src.zip")
#exec(open(os.path.join("C:\Spark\python\pyspark\shell.py")).read())

conf = SparkConf().setMaster('local').setAppName('hello').set("spark.executor.memory", '1g')

sc = SparkContext(conf = conf)

lines = sc.textFile("C:\Spark\README.md")
print("No. of. lines:", lines.count())

lines_error = lines.filter(lambda x: "of" in x)
print("Lines with error:", lines_error.count())

sc.stop()