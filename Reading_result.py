#reading output from the file

import csv  
import os
k="SparkResults"
files = os.listdir(k)
csv_files = filter(lambda x:x[-4:] == '.csv',files)
if(len(csv_files)>0):
    for j in csv_files:
        with open(k+"/"+j) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            tasknumber=0
            for row in csv_reader:
                print  repr(row)