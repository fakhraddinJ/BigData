# Importing basic libraries:
import os
import time
import urllib.request


# Downloding the "airports.csv" file and saving to local folder:
airports_csv = urllib.request.URLopener()
airports_csv.retrieve("http://ourairports.com/data/airports.csv", "Download_csv/airports.csv")
print ("\"airports.csv\" is downloaded and saved to \"Download_csv\" folder")


# A "for loop" to  split the main csv into 10 seperate files and then copying them to the HDFS, repeating in a 10 sec time frame, for 100 times.
for num in range (0,99,1):

    with open('Download_csv/airports.csv', encoding="utf8" , newline='\n') as infp:

        files = [open('Download_csv/airports_parts_%d' %num + '%d.csv' %i, 'w' , encoding="utf8" , newline='\n') for i in range(10)]
        for n, line in enumerate(infp):
            files[n % 10].write(line)
        for f in files:
            f.close()

    infp.close()

    os.system('hadoop fs -put Download_csv/airports_parts_'+str(num)+'*.csv input')
    print ("#"+str(num+1)+" - airports_parts_"+str(num)+"0.csv  ~  airports_parts_"+str(num)+"9.csv are copied to HDFS")

    time.sleep(10)