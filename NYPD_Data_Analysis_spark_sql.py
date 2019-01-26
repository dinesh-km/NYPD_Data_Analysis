from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys
APP_NAME = "NYPD_ANALYZE"

def main(spark,sourcefile):

    reading_data=spark.read.csv(sourcefile,header=True);
    datarequired=reading_data.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')
    f_data=datarequired.filter(col("#DATE").isNotNull() & col("BOROUGH").isNotNull() & col("ZIP CODE").isNotNull() & col("NUMBER OF PERSONS INJURED").isNotNull() & col('NUMBER OF PERSONS KILLED').isNotNull() & col("NUMBER OF PEDESTRIANS INJURED").isNotNull() & col("NUMBER OF PEDESTRIANS KILLED").isNotNull() & col("NUMBER OF CYCLIST INJURED").isNotNull() & col("NUMBER OF CYCLIST KILLED").isNotNull() & col("NUMBER OF MOTORIST INJURED").isNotNull() & col("NUMBER OF MOTORIST KILLED").isNotNull() & col("VEHICLE TYPE CODE 1").isNotNull())
    f_data.registerTempTable("nypdcrash")
    a=spark.sql("select `#date`,count(*) as total from nypdcrash group by `#date` order by total desc  limit 1")
    #a.write.format("csv").save("/user/allasy/final_result/temp1")

    all_queries=a
    b=spark.sql("select borough,sum(int(`NUMBER OF PERSONS KILLED`+`NUMBER OF CYCLIST KILLED`\
                +`NUMBER OF PEDESTRIANS KILLED`+`NUMBER OF MOTORIST KILLED`)) \
                as total from nypdcrash group by borough order by total desc  limit 1")
    #b.write.format("csv").save("/user/allasy/final_result/temp2")
    all_queries = all_queries.union(b)
    c=spark.sql("select `ZIP CODE`,sum(int(`NUMBER OF PERSONS KILLED`+`NUMBER OF CYCLIST KILLED`\
                +`NUMBER OF PEDESTRIANS KILLED`+`NUMBER OF MOTORIST KILLED`))\
                as total from nypdcrash group by `ZIP CODE` order by total desc limit 1")
    #c.write.format("csv").save("/user/allasy/final_result/temp3")
    
    all_queries = all_queries.union(c)
    d=spark.sql("select `VEHICLE TYPE CODE 1`, count(*) as total from nypdcrash group by `VEHICLE TYPE CODE 1` order by total desc  limit 1")
    #d.write.format("csv").save("/user/allasy/final_result/temp4")
    all_queries = all_queries.union(d)
    e=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF PERSONS INJURED`+\
                 `NUMBER OF PEDESTRIANS INJURED`)) as total from nypdcrash group by `Year` order by total desc limit 1")
    #e.write.format("csv").save("/user/allasy/final_result/temp5")
    all_queries = all_queries.union(e)
    f=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF PERSONS KILLED`+\
                `NUMBER OF PEDESTRIANS KILLED`)) as total from nypdcrash group by `Year` order by total desc limit 1")
    #f.write.format("csv").save("/user/allasy/final_result/temp6")
    all_queries = all_queries.union(f)
    g=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF CYCLIST INJURED`+\
                `NUMBER OF CYCLIST KILLED`)) as total from nypdcrash group by `Year` order by total desc limit 1")
    #g.write.format("csv").save("/user/allasy/final_result/temp7")
    all_queries = all_queries.union(g)
    h=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF MOTORIST INJURED`+\
                `NUMBER OF MOTORIST KILLED`)) as Total from nypdcrash group by `Year` order by total desc limit 1")
    all_queries = all_queries.union(h)
    all_queries.write.format("csv").save("/user/allasy/final_result/temp8")
   
    
if __name__ == "__main__":
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   sourcefile=sys.argv[1]
   main(spark,sourcefile)
