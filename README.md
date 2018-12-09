# Data_Analysis_MapReduce_Hadoop
Basic Info :
This project gets data from new york police department about accidents occured throughout multiple year, cleans the data, and process the data

About the file : 
Main file : Cleaning_data.java

Classes used in the file :
1. CleanMap
2. CleanMap2
3. Reducer
4. Cleanup

The concept of chain Mapper is used.

First mapper filters the data based on the requirement. Here, it removes any rows that does not have values in the required columns that needs to be worked upon.

The output from the first mapper is passed to the second mapper where the key value pair gets created and is passed to the reducer for further processing.

Chain Mapper :
