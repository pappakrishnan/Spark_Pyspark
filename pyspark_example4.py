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

if __name__ == "__main__":
    # creating Spark context
    conf = SparkConf().setMaster('local').setAppName('Oops')
    sc = SparkContext(conf = conf)
    sqlcon = SQLContext(sc)
    
    # Loads json file as a dataframe
    data = sqlcon.read.json("C:/Users/vpappakrishnan/Desktop/iphone.json")
    print(data.collect())
    
    # Converts Spark dataframe to Pandas Dataframe
    data = data.toPandas()
    print(data)
    print(data.firstName)

    sc.stop()