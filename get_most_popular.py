import sys
import json
from pyspark.sql.functions import lower
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

#filters the df
#sorts the df
#prints and saves output
def main(inputs, output, location, type, b_count):
    business_df = filter_inputs(inputs, type)
    sorted_output = calculate_averages(business_df, location, b_count)
    print_output(sorted_output, output)

#filters the inputs with review count > 30 and that have the category searching for
def filter_inputs(inputs, type):
    filtered_df = inputs.filter(lower(inputs.categories).contains(type) & (inputs.review_count > 30))
    return filtered_df

#gets the city or state we are looking for
#gets the average stars for the business
#filters out business count less than
def calculate_averages(df, location, b_count):
    group_column = 'city' if location == 'city' else 'state'
    avg_df = df.groupBy(lower(functions.col(group_column)).alias(group_column)).agg(
        functions.avg('stars').alias('avg_stars'),
        functions.count(functions.lit(1)).alias('business_count')
    ).orderBy(functions.col('avg_stars').desc(), functions.col('business_count').desc())
    if b_count:
        avg_df = avg_df.filter(avg_df.business_count >= b_count)
    return avg_df

#prints output and saves it as a json
def print_output(sorted_output, ouput):
    sorted_output.show()
    sorted_rdd = sorted_output.rdd
    print ("Saving json to: " + output)
    sorted_rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    location = sys.argv[3].lower()
    type = sys.argv[4].lower()
    count = sys.argv[5] if len(sys.argv) > 5 else None
    spark = SparkSession.builder.appName('Most Popular').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext.setLogLevel('WARN')
    inputs = spark.read.json(inputs)
    main(inputs, output, location, type, count)
