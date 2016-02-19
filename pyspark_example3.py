# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 14:46:04 2015

@author: vpappakrishnan
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Initializing environmental variables
import os
os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['HADOOP_HOME'] = "C:\Hadoop"
os.environ['JAVA_HOME'] = 'C:\Program Files (x86)\Java\jre1.8.0_60'

# Creating an object for SparkConf() class. Not required when we execute the rest of the code directly in pyspark-shell.
# It tells the program on how to access the cluster / datasource
# Master - Cluster name
# AppName - unique job-submit Identifier
AppName = 'Oops'
master = 'local'
conf = SparkConf().setMaster(master).setAppName(AppName)

sc = SparkContext(conf = conf)
sql_context = SQLContext(sc)
file_loc = "C:/Users/vpappakrishnan/Google Drive/.../...csv"

df = sql_context.read.load("C:\Spark\examples/src/main/resources/people.json", format="json")

# To print the column 'age'
df.select('age').collect()

# To print the whole dataframe
df.collect()

# To print Schema or column information in the Dataframe
df.printSchema()

# To stop sparkcontext
sc.stop()
