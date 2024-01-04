# Yelp Data Analysis: Comparing Tools for Business Insight

This project involves analyzing the Yelp dataset to identify key business trends and preferences, using technologies like Spark, Cassandra, and Pandas to extract insights about popular services and local versus visitor ratings in various locations.

## Table of Contents

- [Introduction](#introduction)
- [Files](#files)
  - [list_of_cities](#list_of_cities)
  - [list_of_states](#list_of_states)
- [Scripts](#scripts)
  - [get_businesses.py](#get_businessespy)
  - [get_most_liked.py](#get_most_likedpy)
  - [get_most_popular.py](#get_most_popularpy)
  - [get_local_popularity_df.py](#get_local_popularity_dfpy)
  - [get_local_popularity_sql.py](#get_local_popularity_sqlpy)
  - [get_local_popularity_pandas.py](#get_local_popularity_pandasp)
  - [get_local_popularity_cass.py](#get_local_popularity_casspy)
  - [load_data_cassandra.py](#load_data_cassandrapy)
  - [load_data_cassandra_spark.py](#load_data_cassandra_sparkpy)

## Introduction

The purpose of this project is to compare how different data analysis tools manipulate the Yelp dataset ([Yelp Dataset](https://www.yelp.com/dataset)) to extract specific data that aids in achieving several key objectives:

1. Identifying certain types of businesses.
2. Finding the highest-rated services.
3. Determining which locations have the best ratings for specific services.
4. Analyzing which businesses receive higher ratings from locals versus out-of-town visitors.

To accomplish these goals, we will utilize various technologies including Spark Dataframes, Spark SQL, Cassandra, and Pandas. All scripts will be executed on the course's cluster to ensure easy reproducibility and facilitate comparison by others who may wish to run them. Detailed instructions on how to run each script are provided below in their respective descriptions.

## Technical Setup

### Obtaining the Yelp Dataset

To work with this project, you will need access to the Yelp dataset. Here are the steps to acquire and set up the dataset:

1. **Download the Dataset**:
   - Visit the Yelp Dataset page at [Yelp Dataset](https://www.yelp.com/dataset).
   - You may need to agree to certain terms of use and create an account if you haven’t already.
   - Download the dataset files. These are typically provided in JSON format.

2. **Extract and Organize**:
   - Once downloaded, extract the files from the compressed folder.
   - Organize the files in a dedicated directory (e.g., `yelp_dataset/`) to keep your workspace tidy.

3. **Upload to the Cluster**:
   - If you are working on a remote cluster (as mentioned in the Introduction), upload the dataset files to the cluster.
   - Ensure the files are placed in an accessible directory and note down the path for use in the scripts.

4. **Verify the Data**:
   - Before proceeding, it’s a good idea to verify the integrity of the data.
   - You can do this by running a simple check on the files to ensure they are complete and uncorrupted.

5. **Data Splitting**:
   - Splitting data into smaller files enables quicker processing, as each cluster core efficiently handles a segment of the data.
   - This method maximizes the cluster's multi-core architecture, streamlining data loading and optimizing tool performance.
   - Depending on your operating system, the method for splitting data may vary, so it's advisable to choose the most suitable technique for your specific OS.


### Preparing Your Environment

Make sure your environment is set up with all the necessary tools mentioned in the project (Spark, Cassandra, Pandas). Installations and configurations might differ based on your system and the cluster you are using.


## Common Notes

- All scripts use data from the Yelp dataset, specifically the `business/`, `user/`, and `review/` files.
- Refer to the `list_of_cities.csv` and `list_of_states.csv` for searchable locations.
- Scripts generally save results in JSON format in the specified output file.
- Most scripts allow specifying a city or state but not both.
- Category searches in scripts are optional and utilize keywords that businesses might be labeled under.

## Files

This project contains the following files:
- `list_of_cities.csv`: This file is a list of cities you can use to search for in the dataset.
- `list_of_states.csv`: This file is a list of states you can use to search for in the dataset.

## Scripts

### get_businesses.py

The goal of this script is to find all or a specific type of business in a city or state. It takes 3 or 4 inputs, depending on whether you are searching for general businesses or a specific business.  
Usage: `spark-submit get_businesses.py business/ output_file "city or state" category(optional)`  
The script will save the results in a JSON format in the output file of your choosing.  

### get_most_liked.py

This script aims to find the most liked services in a specific city or state. It takes 3 or 4 inputs, depending on whether you're searching for general services or a specific service.   
Usage: `spark-submit get_most_liked.py business/ output_file "city or state" category(optional)`  
The script will save the results in a JSON format in the output file of your choosing.  

### get_most_popular.py

This script identifies the highest-rated cities or states for a specific business category. It requires 5 inputs.   
Usage: `spark-submit get_most_popular.py business/ output_file "city or state" category business_count`  
The script will save the results in a JSON format in the output file of your choosing.  

### get_local_popularity_df.py

This script analyzes what businesses in a certain city or state have higher ratings from locals or out-of-town visitors. It takes 5 inputs.   
Usage: `spark-submit get_local_popularity_df.py business/ user/ review/ output_file "city or state"`  
The script will save two files (/local and /outoftown) in a JSON format in the output directory of your choosing.  

### get_local_popularity_sql.py

Similar to the previous script, this script analyzes what businesses in a certain city or state have higher ratings from locals or out-of-town visitors. It takes 5 inputs.   
Usage: `spark-submit get_local_popularity_sql.py business/ user/ review/ output_file "city or state"`  
The script will save two files (/local and /outoftown) in a JSON format in the output directory of your choosing.  

### get_local_popularity_pandas.py

This script, using Pandas for data processing, finds popular businesses in a specified city or state same as the previous two scripts. It is limited to processing only 10,000 lines in the dataframe due to data size constraints.   
Usage: `python3 get_local_popularity_pandas.py business/ user/ review/ output_file "city or state"`  
The script will save two files (/local and /outoftown) in a JSON format in the output directory of your choosing.  

### get_local_popularity_cass.py

Similar to the previous script, this script analyzes what businesses in a certain city or state have higher ratings from locals or out-of-town visitors. It takes 5 inputs.   
Usage:   `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions get_local_popularity_cass.py keyspace output "city or state"`
The script will save two files (/local and /outoftown) in a JSON format in the output directory of your choosing.  
Note: Run either `load_data_cassandra.py` or `load_data_cassandra_spark.py` before this script.  

### load_data_cassandra.py

The purpose of this script is to load business, user, and review data into a Cassandra Cluster using Python and Cassandra.   
Usage: `python3 load_data_cassandra.py business/ user/ review/ keyspace`  

### load_data_cassandra_spark.py

This script loads data into a Cassandra Cluster using Spark and Cassandra.   
Usage: `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions load_data_cassandra_spark.py business/ user/ review/ keyspace`  
