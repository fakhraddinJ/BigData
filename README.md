# BigData
Bia Data practices with Spark
1) wordcount (Spark + Python)
2) Tweet processing (Spark + Python)

    Task 1) Search for the 10 most repeated words in the subject field of the tweets (Top 10 trends)
    
    Task 2) Finding most active user
    
    Task 3) Find shortest tweet 

3) Spark Streaming, Live Airport Analysis (by pyspark) (Stream source: http://ourairports.com/data/airports.csv). The program will look in an HDFS directory every 5 seconds, if there are new files, by Setting a 20 second window and 10 seconds frequency, the program will search the Spanish airports and show how many are small-airport, medium_airport, large_airport and heliport.

4) Spark Streaming, Checking Malaga’s bike parking data via spark streaming technology in a live method, and finding three most nearest parkings to a given coordiation, with more than 10 percent of occupancy. Source URL of the original parking information: http://datosabiertos.malaga.eu/recursos/transporte/EMT/estacionamientos/Estacionamientos.csv .This project is based on two separate implementations of python applications (scripts):

        1.	pyDownloadCSV.py 
        
        2.	hdfs_streaming.py 
        
The first program prepared in python 3.5 environment, called “pyDownloadCSV.py”, is a script responsible to get the CSV file from the URL regularly and then transfer it to the HDFS file system

