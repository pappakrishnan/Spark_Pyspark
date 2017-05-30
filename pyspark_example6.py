# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 14:46:04 2015

@author: vpappakrishnan
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd

# Initializing environmental variables
import os
os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['HADOOP_HOME'] = "C:\Hadoop"
os.environ['JAVA_HOME'] = 'C:\Program Files (x86)\Java\jre1.8.0_60'

def manipulate_data(sc):
    file_loc = "C:/Users/vpappakrishnan/Desktop/iphone.json"
    data = pd.read_csv(file_loc)
    print(len(data))
    
    samples = sc.parallelize(data)
    print(samples.count())

#if __name__ == "__main__":
    # creating Spark context
conf = SparkConf().setMaster('local').setAppName('Hello')
sc = SparkContext(conf = conf)
sqlcon = SQLContext(sc)

# To load as Dataframe
user = sqlcon.read.format("org.apache.spark.sql.cassandra").load(keyspace="training", table="user")

data = sqlcon.read.json("C:/Users/vpappakrishnan/Desktop/iphone.json")
print(data)
#manipulate_data(sc)

sc.stop()