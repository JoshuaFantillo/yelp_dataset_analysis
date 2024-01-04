import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

#get filtered inputs
#sort the filtered inputs and save it as a json
def main(inputs, output, location, city_or_state, topic):
    inputs.cache()
    filtered_df = filtered_inputs(inputs, location, city_or_state, topic)
    if filtered_df:
        sorted_df = sort(filtered_df)
        print("Saving the output as a json file in: " + output)
        sorted_rdd = sorted_df.rdd
        sorted_rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output)

#sorts the dataframe to output the correct columns and in descending order
def sort(df):
    rearranged_df = df.select('name', 'stars', 'review_count', 'city', 'state', 'address','postal_code')
    return rearranged_df.orderBy(functions.col('stars').desc(), functions.col('review_count').desc())

#filters the inputs and drops certain columns
#finds if we are searching for a city or state and if there is a category
#returns the filtered inputs
def filtered_inputs(inputs, location, city_or_state, topic):
    inputs = inputs.drop('attributes', 'business_id','latitude', 'longitude','is_open', 'hours')
    if city_or_state == 'city':
        inputs = inputs.filter(inputs.city == location)
    else:
        inputs = inputs.filter(inputs.state == location)
    if topic:
        filtered_inputs = inputs.filter(functions.col('categories').contains(topic))
        filtered_inputs = filtered_inputs.drop('categories')
        if filtered_inputs.count() == 0:
            print ("There are no " + topic + " categories in this " + city_or_state)
            return None
        else:
            return filtered_inputs
    return inputs.drop('categories')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    location = sys.argv[3]
    city_or_state = 'state'
    if len(location) > 3:
        city_or_state = 'city'
    topic = sys.argv[4] if len(sys.argv)>4 else None
    spark = SparkSession.builder.appName('Get Businesses').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext.setLogLevel('WARN')
    inputs = spark.read.json(inputs)
    main(inputs, output, location, city_or_state, topic)
