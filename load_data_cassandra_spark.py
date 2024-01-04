import sys
import os
import gzip
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row

#creates spark session using spark
#reads business, user, and review datasets into Cassandra
def main(business_path, users_path, review_path, keyspace):
    spark = create_spark_session(keyspace)
    process_directory(spark, business_path, 'business', keyspace)
    process_directory(spark, users_path, 'user', keyspace)
    process_directory(spark, review_path, 'review', keyspace)

#reads dataset into Cassandra
def process_directory(spark, directory_path, table_name, keyspace):
    for f in os.listdir(directory_path):
        data = []
        with gzip.open(os.path.join(directory_path, f), 'rt', encoding='utf-8') as datafile:
            for line in datafile:
                json_data = json.loads(line)
                if table_name == 'business':
                    record = Row(business_id=json_data['business_id'], name=json_data['name'],
                                 address=json_data['address'], city=json_data['city'],
                                 state=json_data['state'], stars=float(json_data['stars']),
                                 review_count=int(json_data['review_count']))
                elif table_name == 'review':
                    record = Row(review_id=json_data['review_id'], user_id=json_data['user_id'],
                                 business_id=json_data['business_id'], stars=float(json_data['stars']))
                elif table_name == 'user' and json_data['review_count'] > 5:
                    record = Row(user_id=json_data['user_id'], name=json_data['name'],
                                 review_count=int(json_data['review_count']))
                else:
                    continue
                data.append(record)

        df = spark.createDataFrame(data)
        df.write.format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("confirm.truncate", True) \
            .options(table=table_name, keyspace=keyspace).save()

#creates spark session with Cassandra
def create_spark_session(keyspace):
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Data With Spark and Cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    return spark

if __name__ == '__main__':
    business_path = sys.argv[1]
    users_path = sys.argv[2]
    review_path = sys.argv[3]
    keyspace = sys.argv[4]
    main(business_path, users_path, review_path, keyspace)
