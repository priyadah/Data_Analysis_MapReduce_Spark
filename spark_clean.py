import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType,StructType,StructField
from pyspark.sql import DataFrame

def createOneDataFrame(finaldfs):
    toBeReturned =[]
    for dat in finaldfs:
        toBeReturned.append(dat.select('Task','result'));
    return toBeReturned;

if __name__ == "__main__":

    sc = SparkContext()
    sparkSession = SparkSession.builder.appName("spark-project-2").getOrCreate()

    df=sparkSession.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv(sys.argv[1])

   #selecting the columns that are required for cleanup
    dff=df.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')

    # removing all the rows that have null value in the above selected columns
    dfff=dff.na.drop()

    # saving the clean up file
    dfff.coalesce(1).write.format('com.databricks.spark.csv').save(sys.argv[2],header = 'true')
    dfff.registerTempTable("tempTable1")

   #sql queries to get the desired data for different columns
    Date = sparkSession.sql("select 'date' as Task,count(*) as total1,`#date` as result from tempTable1 group by `#date` order by  total1 desc limit 1")

   #sql query to get the borough with maximum fatality
    Borough = sparkSession.sql("select 'borough' as Task,`BOROUGH` as result,sum(int(int(`NUMBER OF PERSONS KILLED`)+int(`NUMBER OF PEDESTRIANS KILLED`)+int(`NUMBER OF CYCLIST KILLED`)+int(`NUMBER OF MOTORIST KILLED`))) as total1 from tempTable1 group by result order by total1 desc  limit 1")

    #sql query to get the zip code with maximum fatality
    Zip_Code = sparkSession.sql("select 'zip_code' as Task,`ZIP CODE` as result,sum(int(int(`NUMBER OF PERSONS KILLED`)+int(`NUMBER OF PEDESTRIANS KILLED`)+int(`NUMBER OF CYCLIST KILLED`)+int(`NUMBER OF MOTORIST KILLED`))) as total2 from tempTable1 group by result order by total2 desc  limit 1")

    #sql query to get the vehicle type code with maximum accident
    Vehicle_TyCD =sparkSession.sql("select 'vehicle_tycd' as Task,`VEHICLE TYPE CODE 1` as result,count(*) as total3 from tempTable1 group by result order by total3 desc  limit 1")
    
    #sql query to get year in which maximum Number Of Persons and Pedestrians Injured
    Year1 = sparkSession.sql("select 'year2' as Task,SUBSTRING(`#DATE`,7,4) as result,sum(int(`NUMBER OF PERSONS INJURED`)) as total8,sum(int(`NUMBER OF PEDESTRIANS INJURED`)) as total9 from tempTable1 group by result order by total8 desc,total9 desc limit 1")
    
    #sql query to get Year in which maximum Number Of Persons and Pedestrians Killed
    Year2 = sparkSession.sql("select 'year3' as Task,SUBSTRING(`#DATE`,7,4) as result,sum(int(`NUMBER OF PERSONS KILLED`)) as total6,sum(int(`NUMBER OF PEDESTRIANS KILLED`)) as total7 from tempTable1 group by result order by total6 desc,total7 desc limit 1")
    
    #sql query to get Year in which maximum Number Of Cyclist Injured and Killed (combined)
    Year3 = sparkSession.sql("select 'year3' as Task,SUBSTRING(`#DATE`,7,4) as result,sum(int(`NUMBER OF CYCLIST INJURED`)+int(`NUMBER OF CYCLIST KILLED`)) as total4 from tempTable1 group by result order by total4 desc limit 1")
    
    #sql query to get Year in which maximum Number Of Motorist Injured and Killed (combined)   
    Year4 = sparkSession.sql("select 'year4' as Task,SUBSTRING(`#DATE`,7,4) as result,sum(int(`NUMBER OF MOTORIST INJURED`)+int(`NUMBER OF MOTORIST KILLED`)) as total5 from tempTable1 group by result order by total5 desc limit 1")

    finalDfs = createOneDataFrame([Date,Borough,Zip_Code,Vehicle_TyCD,Year1,Year2,Year3,Year4])
    dffff = reduce(DataFrame.unionAll, finalDfs)
    #saving the final output
    dffff.coalesce(1).write.csv(sys.argv[3])