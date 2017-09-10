# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 20:48:25 2017

@author: Fakhraddin Jaf
"""

#Importing required libraries :
import urllib.request
import csv
import time 
from subprocess import call

#Creating a numeric variable to control file naming and loop iterations:
count = 1

#starts a forever loops to execute the commands and download the CSV file, until interruption from user side:
while True:
    #Source URL of the original parking information:
    url = 'http://datosabiertos.malaga.eu/recursos/transporte/EMT/estacionamientos/Estacionamientos.csv'
    
	#starts an HTTPS request to get CSV:
	response = urllib.request.urlopen(url)
	
	#Reads the HTTPS response into an UTF-8 decoded text variable:
    data = response.read()      
    text = data.decode('utf-8') 
	
	#Reads the text into CSV format:
    cr = csv.reader(text.splitlines(), delimiter=',', skipinitialspace=True)
	
	# Writes the csv.reader data into a new CSV file with the delimiter “#”, 
	# to skip reading “,” inside CSV cells and avoid creating unwanted CSV cells, 
	# And each loop iteration creates new CSV file:
    with open('csv/Estacionamientos'+str(count)+'.csv', 'w', newline='') as fp:
        a = csv.writer(fp, delimiter='#')
        a.writerows(cr)
        fp.close()
		
	# loop execution pauses and waits for 2 seconds, then continues:
	time.sleep(2)
	
	# Executes a Linux shell inside the script to copy the newly downloaded file
	# into the HDFS system for Spark processing:
    call(["hadoop" , "fs", "-put" , "csv/Estacionamientos"+str(count)+".csv" , "input/"])
	
	# prints into screen after copying file into HDFS :
    print('Estacionamientos'+str(count)+'.csv downloaded and saved to HDFS\n')
    
	#Increments the Count value, waits for 5 seconds , and goes back to the beginning of the loop:
	count = count + 1
    time.sleep(5)



