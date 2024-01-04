import sys
import json
from pyspark.sql.functions import lower
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

#filters the inputs and then sorts them and saves the json file
def main(inputs, output, location, city_or_state, type):
    filtered_inputs = filter_inputs(inputs, location, city_or_state, type)
    sorted_output = average_ratings(filtered_inputs, type)
    sorted_output.show()
    sorted_rdd = sorted_output.rdd
    print ("Saving json to: " + output)
    sorted_rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output)

#finds if we are looking for a city or state
#gets businesses with review counts over 10
#gets businesses with specific category
def filter_inputs(inputs, location, city_or_state, type):
    if city_or_state == 'city':
        business_df = inputs.filter(inputs.city == location)
    else:
        business_df = inputs.filter(inputs.state == location)

    business_df = business_df.filter(business_df.review_count > 10)
    if type:
        business_df = business_df.filter(lower(business_df.categories).contains(type))
    return business_df

#gets average rating of each business type and returns the df
def average_ratings(business_df, type):
    exploded_df = business_df.withColumn('category', functions.explode(functions.split(functions.col('categories'), ', ')))

    avg_ratings = exploded_df.groupBy('category').agg(functions.avg('stars').alias('avg_rating'),functions.count('stars').alias('count'))

    sorted_avg_ratings = avg_ratings.orderBy(functions.col('avg_rating').desc())
    if type==None:
        sorted_avg_ratings = sorted_avg_ratings.filter(functions.col('count') >= 10)
    else:
        sorted_avg_ratings = sorted_avg_ratings.filter(functions.col('count') >= 3)
    return sorted_avg_ratings

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    location = sys.argv[3]
    city_or_state = 'state'
    if len(location) > 3:
        city_or_state = 'city'
    type = sys.argv[4].lower() if len(sys.argv) > 4 else None
    spark = SparkSession.builder.appName('Most Liked').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext.setLogLevel('WARN')
    inputs = spark.read.json(inputs)
    main(inputs, output, location, city_or_state, type)
