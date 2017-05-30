# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 14:46:04 2015

@author: vpappakrishnan
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#import pandas as pd

# Initializing environmental variables
import os
os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['HADOOP_HOME'] = "C:\Hadoop"
os.environ['JAVA_HOME'] = 'C:\Program Files (x86)\Java\jre1.8.0_60'

def manipulate_data(sc):
    data = sc.textFile("C:\Spark\README.md")        
    error_count = data.filter(lambda x: 'the' in x)
    print(error_count.count())

def filter_data(data):
    return 'of the' in data

#def count_of(sc, data):
#data = sc.parallelize(data).
#    
#        
#    
#    return "of" in data

if __name__ == "__main__":
    # creating Spark context
    conf = SparkConf().setMaster('local').setAppName('Oops')
    sc = SparkContext(conf = conf)
    sqlcon = SQLContext(sc)
    
    # Method 1 - passing the SparkContext (object)
    manipulate_data(sc)
    
    # Method 2 - passing the function to be applied
    new_data = sc.textFile("C:\Spark\README.md")
    new_data = new_data.filter(filter_data)
    print(new_data.count())
    
#    of_count = count_of(sc, new_data)    
#    print(of_count)
#    print(sc.parallelize(new_data).countByValue('of'))
    
    sc.stop()