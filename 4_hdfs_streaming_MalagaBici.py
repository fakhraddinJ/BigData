# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 10:30:20 2017

@author: Fakhraddin Jaf
"""
#Importing basic libraries:
from __future__ import print_function
import sys
import math

#Importing PySpark libraries for Spark Streaming:
from pyspark import SparkContext
from pyspark import RDD
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    
	#the program checks if the spark_submit command has introduced correctly: 
    if len(sys.argv) != 4:
        print("Usage: hdfs_streaming.py <directory> <latitude> <longitude>", file=sys.stderr)
        exit(-1)
    
	# Create a local Streaming Context with two working thread and batch interval of 15 second:
    sc = SparkContext(appName="PySparkStreaming")
    ssc = StreamingContext(sc, 15)
    
	# Looks inside the input directory (as streaming source) and creates a DStream: 
    lines = ssc.textFileStream(sys.argv[1])
	
	# converting input geo-location parameters to float values: 
    LAT = float(sys.argv[2])
    LON = float(sys.argv[3])

	#1) Splits each line into words,
    #2) filtering those lines that have ID to skip CSV header,
    #3) filtering those lines that have more than %10 occupation:
    parts = lines.map(lambda l: l.encode("ascii", "ignore").split("#"))\
                                    .filter(lambda p: (p[0].isdigit()))\
	       .filter(lambda p: (int(p[8])*100)/(int(p[7])+int(p[8])) > 10)

		   
    #1) Transforms newly filtered lines into the new sorted list, based on nearest location,
    #2) and #3) are the lines representing formula to calculate distance between two locations:
    sorted_by_nearest_Loc = parts.transform(lambda rdd: rdd.sortBy\
                    (lambda x: math.sqrt(((LAT-(float(x[9])))**2)+\
	                   ((LON-(float(x[10])))**2)), ascending=True))

    # Prints first three rows (Bike parking location info ) of the list:
    sorted_by_nearest_Loc.pprint(3)
 
    # Start the computation:
    ssc.start()
	
	# Wait for the computation to terminate:
    ssc.awaitTermination()