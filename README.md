# BigData
Bia Data practices with Spark
1) wordcount (Spark + Python)
2) Tweet processing (Spark + Python)

    Task 1) Search for the 10 most repeated words in the subject field of the tweets (Top 10 trends)
    
    Task 2) Finding most active user
    
    Task 3) Find shortest tweet 

3) Spark Streaming, Live Airport Analysis (by pyspark) (Stream source: http://ourairports.com/data/airports.csv). The program will look in an HDFS directory every 5 seconds, if there are new files, by Setting a 20 second window and 10 seconds frequency, the program will search the Spanish airports and show how many are small-airport, medium_airport, large_airport and heliport.

4) Spark Streaming, Checking Malaga’s bike parking data via spark streaming technology in a live method, and finding three most nearest parkings to a given coordiation, which have more than 10 percent of occupancy. Source URL of the original parking information: http://datosabiertos.malaga.eu/recursos/transporte/EMT/estacionamientos/Estacionamientos.csv .This project is based on two separate implementations of python applications (scripts):

        1.	4_pyDownloadCSV_MalagaBici.py 
        
        2.	4_hdfs_streaming_MalagaBici.py 
        
The first program prepared in python 3.5 environment, called “4_pyDownloadCSV_MalagaBici.py”, is a script used to get the CSV file from the URL regularly and then transfer it to the HDFS file system. Second program developed in PySpark environment, called “4_hdfs_streaming_MalagaBici.py”, is a script submitted with Spark Streaming, and is responsible to load newly transferred files into streaming contexts from HDFS, and then transform them for further manipulation. for example:

       $ spark_submit 4_hdfs_streaming_MalagaBici.py input 36.717320 -4.418990 
       
       •	directory in HDFS :  input       
       •	latitude: 36. 717320
       •	longitude: -4.418990

The next step after the Spark Streaming has started successfully, to put files into the source directory, the "4_pyDownloadCSV_MalagaBici.py" script will be executed, in a separate terminal window:  

       $ python3.5 4_pyDownloadCSV_MalagaBici.py     

![Screenshot](/BigData/4_hdfs_streaming_MalagaBici.jpg?raw=true "Optional Title")

