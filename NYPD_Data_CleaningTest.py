
## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys
APP_NAME = "NYPD_Analysis_Cleaning"

def main(spark,filename):
    df_total=spark.read.csv(filename,header=True);
    required_data=df_total.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')
    final_data=required_data.filter(col("#DATE").isNotNull() & col("BOROUGH").isNotNull() & col("ZIP CODE").isNotNull() & col("NUMBER OF PERSONS INJURED").isNotNull() & col('NUMBER OF PERSONS KILLED').isNotNull() & col("NUMBER OF PEDESTRIANS INJURED").isNotNull() & col("NUMBER OF PEDESTRIANS KILLED").isNotNull() & col("NUMBER OF CYCLIST INJURED").isNotNull() & col("NUMBER OF CYCLIST KILLED").isNotNull() & col("NUMBER OF MOTORIST INJURED").isNotNull() & col("NUMBER OF MOTORIST KILLED").isNotNull() & col("VEHICLE TYPE CODE 1").isNotNull())
    final_data.write.format("csv").save("/user/katakadh/train")
    
    

if __name__ == "__main__":
   # Configure Spark
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   filename = sys.argv[1]
   # Execute Main functionality
   main(spark, filename)
