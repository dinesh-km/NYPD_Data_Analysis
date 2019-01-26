#Data Analysis of NYPD using Spark and MapReduce
...........................................................................................................

Note:Task2 alone is sufficient to clean and get the data this is OPTIONAL 

#Task1 (Cleaning data) in pyspark

by running NYPD_Data_CleaningTest.py we'll get cleaned data in /user/katakadh/train

command is::

==>(if running from some other directory)
spark-submit /home/katakadh/data/NYPD_Data_CleaningTest.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt

==>If running from katakadh/data
spark-submit NYPD_Data_CleaningTest.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt
------------------------------------------------------------------------------------------------------------

#Task2 (Analysis of data) in pyspark using spark->sql

running NYPD_Data_Analysis_spark_sql.py will execute the 8 queries written in spark->sql and writes them to the destination directory which I have specified as <destdirectory="sparkresult"> in my katakadh Directory

command is::
==>(if running from some other directory)
spark-submit /home/katakadh/data/NYPD_Data_Analysis_spark_sql.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt

==>If running from katakadh/data
spark-submit NYPD_Data_Analysis_spark_sql.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt

Note:written cleaning and analysis in same file.
--------------------------------------------------------------------------------------------------------------

#Task2 (Analysis of data) in pyspark using spark->rdd

command is::
==>(if running from some other directory)
spark-submit --master yarn --deploy-mode client --executor-memory 1g --name task2 --conf "spark.app.id=task2" task2.py /user/katakadh/train
--------------------------------------------------------------------------------------------------------------


##Task3 in python

read the files which I have written in <sparkresult> folder of my katakadh directory in task 2 and 
printing the same output to output.txt where we are running the python


For output to output.txt
python /home/katakadh/data/FinalResult.py >output.txt

For output on terminal please run::
python /user/katakadh/data/FinalResult.py


Final Output from spark::
Date on maximum number of accidents took place is:['01/21/2014', '842']
Borough with maximum count of accident fatality:['BROOKLYN', '663']
Zip with maximum count of accident fatality:['11236', '54']
Which vehicle type is involved in maximum accidents:['PASSENGER VEHICLE', '502983']
Year in which maximum Number Of Persons and Pedestrians Injured:['2013', '52791']
Year in which maximum Number Of Persons and Pedestrians Killed:['2013', '345']
Year in which maximum Number Of Cyclist Injured and Killed (combined):['2015', '3876']
Year in which maximum Number Of Motorist Injured and Killed (combined):['2013', '27622']


---------------------------------------------------------------------------------------------------------------	

#task2 in MapReduce using java

running the task2 which is cleaned by using spark will give us the final result in FinalResult folder of my katakadh Directory or any other that we specify

yarn jar /home/katakadh/data/MRSumbission.jar com.na.com.DataAnalysis /user/katakadh/train /user/katakadh/FinalResult

-----------------------------------------------------------------------------------------------------------------

#task3 in MapReduce using java

Running a javafile present in my data directory will read user/katakadh/MRResults where I have written my output from reduce will give the finalresult.

Command is::
javac /home/katakadh/FinalResult.java

We are writing it to output.txt
java -cp /home/katakadh/ FinalResult >output.txt

If we want to see output on terminal
java -cp /home/katakadh/ FinalResult


Final Output from MapReduce is::

01/21/2014	842
BROOKLYN	663
11236	54
PASSENGER VEHICLE	502983
2013	52791
2013	345
2015	3876
2013	27622
