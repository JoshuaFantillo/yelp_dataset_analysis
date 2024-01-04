import sys
import json
from pyspark.sql.functions import lower, row_number, desc, col, isnull
from pyspark.sql.window import Window
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

#gets the business, user, review df
#gets the users local city
#gets the local business reviews with reviews from locals and out of town
#drops unwanted columns
#caches business reviews
#displays the output
def main(business, users, review, output, location):
    business = business.select('business_id', 'name', 'address', 'city', 'state', 'stars', 'review_count', 'postal_code')
    users = users.select('user_id', 'name', 'review_count', 'yelping_since')
    review = review.select('review_id', 'user_id', 'business_id', 'stars')
    users = get_local_city(business, users, review)
    business_reviews = get_local_business_review(business, users, review)
    business_reviews = business_reviews.drop('business_id', 'postal_code', 'stars', 'review_count')
    business_reviews.cache()
    display_output(business_reviews, output, location)

#calculates users local city based on where majority of their reviews are located
def get_local_city(business, users, review):
    active_users = users.filter(users.review_count > 5)
    user_reviews = review.join(active_users, 'user_id').join(business, 'business_id')
    user_city_counts = user_reviews.groupBy('user_id', 'city').count()

    windowSpec = Window.partitionBy("user_id").orderBy(desc("count"))
    user_max_review_counts = user_city_counts.withColumn("row_number", row_number().over(windowSpec))\
                                             .filter("row_number = 1")\
                                             .drop('count', 'row_number')

    return active_users.join(user_max_review_counts, 'user_id').withColumnRenamed('city', 'local_city')

#adds local reviews and stars and out of town reviews and stars to dataframe
def get_local_business_review(business, users, review):
    review = review.withColumnRenamed('stars', 'review_stars').withColumnRenamed('user_id', 'review_user_id').withColumnRenamed('business_id', 'review_business_id')
    business = business.withColumnRenamed('name', 'business_name').withColumnRenamed('stars', 'business_stars').withColumnRenamed('review_count', 'business_review_count')
    review_with_user = review.join(users, review.review_user_id == users.user_id)
    business_reviews = business.join(review_with_user, business.business_id == review_with_user.review_business_id)

    return business_reviews.groupBy(business.columns)\
                           .agg(functions.count(functions.when(business_reviews.local_city == business_reviews.city, 1)).alias('reviews_from_locals'),
                                functions.count(functions.when(business_reviews.local_city != business_reviews.city, 1)).alias('reviews_from_out_of_town'),
                                functions.avg(functions.when(business_reviews.local_city == business_reviews.city, review_with_user.review_stars)).alias('average_stars_from_locals'),
                                functions.avg(functions.when(business_reviews.local_city != business_reviews.city, review_with_user.review_stars)).alias('average_stars_from_out_of_town'))

#finds if location is city or state and filters accoridngly
#gets the local higher df and outoftown hgiher df
#organizes dataframe to only show columns we want
#prints data frame and saves as json
def display_output(output_df, output, location):
    if location:
        if len(location) <= 3:
            output_df = output_df.filter(output_df.state == location)
        else:
            output_df = output_df.filter(output_df.city == location)

    locals_higher_df = output_df.filter(((col("average_stars_from_locals") > col("average_stars_from_out_of_town")) | isnull(col("average_stars_from_out_of_town"))) & (col("reviews_from_locals") > 3))\
                                  .orderBy(col("average_stars_from_locals").desc())
    outoftown_higher_df = output_df.filter(((col("average_stars_from_out_of_town") >= col("average_stars_from_locals")) | isnull(col("average_stars_from_locals"))) & (col("reviews_from_out_of_town") > 1))\
                                  .orderBy(col("average_stars_from_out_of_town").desc())

    locals_higher_df = locals_higher_df.drop('reviews_from_out_of_town','business_stars')
    outoftown_higher_df = outoftown_higher_df.drop('reviews_from_locals','business_stars')
    locals_higher_df = locals_higher_df.select(["business_name", "average_stars_from_locals", "reviews_from_locals", "city", "state", "address", "average_stars_from_out_of_town"])
    outoftown_higher_df = outoftown_higher_df.select(["business_name", "average_stars_from_out_of_town", "reviews_from_out_of_town","city", "state", "address", "average_stars_from_locals"])
    locals_higher_df.show()
    outoftown_higher_df.show()

    print("Saving locals json to: " + output + '/local')
    locals_higher_df.rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output + 'local')
    print("Saving outoftown json to: " + output +'/outoftown')
    outoftown_higher_df.rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output + '/outoftown')

if __name__ == '__main__':
    business = sys.argv[1]
    users = sys.argv[2]
    review = sys.argv[3]
    output = sys.argv[4]
    location = sys.argv[5] if len(sys.argv)>5 else None
    spark = SparkSession.builder.appName('Most Liked').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext.setLogLevel('WARN')
    business = spark.read.json(business)
    users = spark.read.json(users)
    review = spark.read.json(review)
    main(business, users, review, output, location)
