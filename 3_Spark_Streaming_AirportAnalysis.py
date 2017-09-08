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
    if len(sys.argv) != 2:
        print("Usage: AirportAnalysis.py <directory>", file=sys.stderr)
        exit(-1)

    # Creates a local Streaming Context with two working thread and batch interval of 5 second:
    sc = SparkContext(appName="PySpark_HDFS-Streaming_ES_airports")
    ssc = StreamingContext(sc, 5)

    # Looks inside the input directory (as streaming source) and creates a DStream:
    lines = ssc.textFileStream(sys.argv[1])

    # Listing Spanish Airports and their quantity in following manner: 
    # By setting a 20 second window and 10 seconds frequency, 
    # the program searches the Spanish airports and show hows many are 
    # small-airport, medium_airport, large_airport and heliport.												
    ES_airports = lines.map(lambda l: l.encode("ascii", "ignore").split(","))\
                     .filter(lambda p: p[0].isdigit() and p[8] == '"ES"' and \
		                   p[2] != '"seaplane_base"' and p[2] != '"closed"' )\
                                                 .map(lambda p: p[2].strip())\
                                                       .map(lambda x: (x, 1))\
         .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 20, 10)

    # Prints the result:
    ES_airports.pprint()

    # Sets the checkpoint directory, to make the application fault - tolerant:
    # "checkpoint" is a directory inside HDFS
    ssc.checkpoint("checkpoint")   
	
    # Starts the computation:
    ssc.start()

    # Waits for the computation to terminate:
    ssc.awaitTermination()