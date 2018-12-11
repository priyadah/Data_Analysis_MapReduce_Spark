# Data_Analysis_MapReduce_Hadoop_&&_Spark
Basic Info :
This project gets data from new york police department about accidents occured throughout multiple year, cleans the data, and process the data. The objective is achieved by using both MapReduce and Spark.

MapReduce

About the file : 
Main file : Cleaning_data.java

How to run the file :
1 . compile the .java file
2 . create a jar file using the command "jar -cvf javafilename.jar -C source_path_of_file destination_path_of_file"
3 . run the jar file using command "hadoop javafilename.jar javafilename(without .java) <input file path> <output file path>

Classes used in the file :
1. CleanMap
2. CleanMap2
3. Reducer
4. Cleanup

The concept of chain Mapper is used.

First mapper filters the data based on the requirement. Here, it removes any rows that does not have values in the required columns that needs to be worked upon.

The output from the first mapper is passed to the second mapper where the key value pair gets created and is passed to the reducer for further processing.

Chain Mapper :
The concept of Chain Mapper is used in this project just for a better understanding. This project can be done without a chain mapper as well.
How to use chain Mapper can be understood from the following link : https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapred/lib/ChainMapper.html

Few things needs to be noted. The definitions of mapper1 , mapper2, reducer1, mapper 3,reducer2, etc, all remains same just like the case when one mapper and one reducer is being used. The only difference comes in the configuration part where while setting the chain mapper using addMapper, we need to make sure that the output of the first mapper goes as the input of the second mapper and so on. Note :Only in the configuration i.e. main class.

Benefits of using a chained MapReduce job :
1 . The key functionality of this feature is that the Mappers in the chain do not need to be aware that they are executed in a chain. This enables having reusable specialized Mappers that can be combined to perform composite operations within a single task.
2.  When MapReduce jobs are chained data from immediate mappers is kept in memory rather than storing to disk so that another mapper in chain doesn't have to read data from disk. Immediate benefit of this pattern is a dramatic reduction in disk IO.

Reducer : The reducer gets the key and all values associated with that key from the mapper2. The IntWritable is iterable since for one key there are multiple values associated with it. The output of reducer is stored in a hashmap which further is used in the cleanup method of reducer to get the maximum amongst different columns.

Spark 

About the file : 
Main file --> Spark_clean.py

DataFrames are used to store data in a tabular fashion with/without header. If the input file contains header, data is processed , however, if it does not contain header, a header is added to it using the toDF method, which provides a way to rename the default column(_c0,_c1 etc) to the desired column name.

SQL queries are used to fetch the data based on the required conditions.
