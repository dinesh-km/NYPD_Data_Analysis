# spark-submit --master yarn --deploy-mode client --executor-memory 1g --name task2 --conf "spark.app.id=task2" task2.py /user/katakadh/task1_result/part-* 8

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys



def main(spark, filename):
    # dataframe with data from csv file from task 1
    df = spark.read.csv(filename, header=False)
    df = spark.read.csv("/user/ravindrh/task1_result/part-*", header=False)
    
    # convert number columns from string to int
    # convert dataframe to rdd
    dataRDD = df.select(df._c0,df._c1,df._c2,df._c3.cast("int"),df._c4.cast("int"),df._c5.cast("int"),df._c6.cast("int"),df._c7.cast("int"),df._c8.cast("int"), df._c9.cast("int"),df._c10.cast("int"),df._c11).rdd
    
    # map with date as key and sum of people injured and killed as value
    # reduce by adding all values of same date
    
    # 1) Date on which maximum number of accidents took place.
    max_accidents_date = dataRDD.map(lambda row: (row[0],1)).reduceByKey(add).max(lambda x: x[1])
    
    # 2) Borough with maximum count of accident fatality
    max_fatality_borough = dataRDD.map(lambda row: (row[1], row[4]+row[6]+row[8]+row[10])).reduceByKey(add).max(lambda x: x[1])

    # 3) Zip with maximum count of accident fatality
    max_fatality_zip = dataRDD.map(lambda row: (row[2], row[4]+row[6]+row[8]+row[10])).reduceByKey(add).max(lambda x: x[1])

    # 4) Which vehicle type is involved in maximum accidents
    max_accidents_vehicle = dataRDD.map(lambda row: (row[11],1)).reduceByKey(add).max(lambda x: x[1])    

    # 5) Year in which maximum Number Of Persons and Pedestrians Injured
    max_person_ped_injured = dataRDD.map(lambda row: (row[0][6:],row[3]+row[5])).reduceByKey(add).max(lambda x: x[1])  

    # 6) Year in which maximum Number Of Persons and Pedestrians Killed
    max_person_ped_killed = dataRDD.map(lambda row: (row[0][6:],row[4]+row[6])).reduceByKey(add).max(lambda x: x[1])  

    # 7) Year in which maximum Number Of Cyclist Injured and Killed (combined)
    max_cyclist_injured_killed = dataRDD.map(lambda row: (row[0][6:],row[7]+row[8])).reduceByKey(add).max(lambda x: x[1])

    # 8) Year in which maximum Number Of Motorist Injured and Killed (combined)
    max_motorist_injured_killed = dataRDD.map(lambda row: (row[0][6:],row[9]+row[10])).reduceByKey(add).max(lambda x: x[1])  
    
    # write to location
    final_df = spark.createDataFrame([max_accidents_date,max_fatality_borough,max_fatality_zip,max_accidents_vehicle,max_person_ped_injured,max_person_ped_killed,max_cyclist_injured_killed,max_motorist_injured_killed],["Result","Quantity"])
    final_df.write.format("csv").option("delimiter", "\t").save("/user/katakadh/task2/")

if __name__ == "__main__":
    # Configure Spark
    spark = SparkSession.builder.master("local").appName("Task 2").getOrCreate()
    filename = sys.argv[1]
    # Execute Main functionality
    main(spark, filename)
