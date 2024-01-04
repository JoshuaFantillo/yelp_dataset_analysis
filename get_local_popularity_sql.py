import sys
import json
from pyspark.sql.functions import lower
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

#get business, review, user dataframe
#get local city for users
#get business dataframe that has local and out of town reviews and stars
#drop unwanted columns
#cache the dataframe
#display and save datafrme
def main(business, users, review, output, location):
    business = business.select('business_id', 'name', 'address', 'city', 'state', 'stars', 'review_count', 'postal_code')
    users = users.select('user_id', 'name', 'review_count', 'yelping_since')
    review = review.select('review_id', 'user_id', 'business_id', 'stars')
    users = get_local_city(business, users, review)
    business_reviews = get_local_business_review(business, users, review)
    business_reviews = business_reviews.drop('business_id', 'postal_code', 'stars', 'review_count')
    business_reviews.cache()
    display_output(business_reviews, output, location)

#gets the dataframe that has local and out of town reviews on it
def get_local_business_review(business, users, review):
    business.createOrReplaceTempView("business")
    users.createOrReplaceTempView("users")
    review.createOrReplaceTempView("review")

    spark.sql("""
        SELECT r.*, u.local_city
        FROM review r
        JOIN users u ON r.user_id = u.user_id
    """).createOrReplaceTempView("review_with_user")

    spark.sql("""
        SELECT
            b.business_id,
            b.name,
            b.address,
            b.city,
            b.state,
            b.stars,
            b.review_count,
            b.postal_code,
            COUNT(CASE WHEN r.local_city = b.city THEN 1 END) AS reviews_from_locals,
            COUNT(CASE WHEN r.local_city != b.city THEN 1 END) AS reviews_from_out_of_town,
            AVG(CASE WHEN r.local_city = b.city THEN r.stars END) AS average_stars_from_locals,
            AVG(CASE WHEN r.local_city != b.city THEN r.stars END) AS average_stars_from_out_of_town
        FROM business b
        JOIN review_with_user r ON b.business_id = r.business_id
        GROUP BY b.business_id, b.name, b.address, b.city, b.state, b.stars, b.review_count, b.postal_code
    """).createOrReplaceTempView("business_with_review_stats")

    return spark.sql("SELECT * FROM business_with_review_stats")

#gets the users local city based on where the majority of their reviews are located
def get_local_city(business, users, review):
    business.createOrReplaceTempView("business")
    users.createOrReplaceTempView("users")
    review.createOrReplaceTempView("review")
    spark.sql("""
        SELECT *
        FROM users
        WHERE review_count > 5
    """).createOrReplaceTempView("active_users")

    spark.sql("""
        SELECT u.user_id, b.city, COUNT(*) as review_count
        FROM active_users u
        JOIN review r ON u.user_id = r.user_id
        JOIN business b ON r.business_id = b.business_id
        GROUP BY u.user_id, b.city
    """).createOrReplaceTempView("user_city_review_counts")

    spark.sql("""
        SELECT user_id, MAX(review_count) as max_review_count
        FROM user_city_review_counts
        GROUP BY user_id
    """).createOrReplaceTempView("user_max_review_counts")

    spark.sql("""
        SELECT u.user_id, FIRST(c.city) as local_city
        FROM user_max_review_counts u
        JOIN user_city_review_counts c ON u.user_id = c.user_id AND u.max_review_count = c.review_count
        GROUP BY u.user_id
        HAVING COUNT(*) = 1  -- This ensures no ties
    """).createOrReplaceTempView("local_city")
    user_details_with_city = spark.sql("""
        SELECT a.user_id, a.name, a.review_count, a.yelping_since, c.local_city
        FROM active_users a
        JOIN local_city c ON a.user_id = c.user_id
    """)
    return user_details_with_city

#organizes the output and prints and saves it as a json
def display_output(output_df, output, location):
    output_df.createOrReplaceTempView("output_view")
    if location:
        location_filter = "state = '{}'".format(location) if len(location) <= 3 else "city = '{}'".format(location)
        filtered_sql = "SELECT * FROM output_view WHERE " + location_filter
        output_df = spark.sql(filtered_sql)

    output_df.createOrReplaceTempView("filtered_output")
    locals_sql = """
        SELECT name, average_stars_from_locals, reviews_from_locals, address, city, state, average_stars_from_out_of_town
        FROM filtered_output
        WHERE (average_stars_from_locals > average_stars_from_out_of_town OR average_stars_from_out_of_town IS NULL) AND reviews_from_locals > 5
        ORDER BY average_stars_from_locals DESC
    """
    locals_higher_df = spark.sql(locals_sql)

    outoftown_sql = """
        SELECT name, average_stars_from_out_of_town, reviews_from_out_of_town, address, city, state, average_stars_from_locals
        FROM filtered_output
        WHERE (average_stars_from_locals <= average_stars_from_out_of_town OR average_stars_from_locals IS NULL) AND reviews_from_out_of_town > 5
        ORDER BY average_stars_from_out_of_town DESC
    """
    outoftown_higher_df = spark.sql(outoftown_sql)
    locals_higher_df.show()
    outoftown_higher_df.show()

    print("Saving locals json to: " + output + '/local')
    locals_higher_df.rdd.map(lambda row: json.dumps(row.asDict())).saveAsTextFile(output + '/local')

    print("Saving outoftown json to: " + output + '/outoftown')
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
