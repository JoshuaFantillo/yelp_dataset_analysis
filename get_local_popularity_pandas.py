import os
import sys
import json
import pandas as pd
import gzip
from io import StringIO

#gets the local city of the user based on where the majority of their reviews are
def get_local_city(users, review, business):
    active_users = users[users['review_count'] > 5]
    user_reviews = pd.merge(review, active_users, on='user_id')
    user_business_reviews = pd.merge(user_reviews, business[['business_id', 'city']], on='business_id')
    user_city_counts = user_business_reviews.groupby(['user_id', 'city']).size().reset_index(name='counts')
    user_max_review_counts = user_city_counts.sort_values('counts', ascending=False).drop_duplicates('user_id')
    return pd.merge(active_users, user_max_review_counts[['user_id', 'city']], on='user_id').rename(columns={'city': 'local_city'})

#gets a dataframe that has local and out of town user reviews and stars
def get_local_business_review(business, users, review):
    review_with_user = pd.merge(review, users, left_on='user_id', right_on='user_id')
    business_reviews = pd.merge(business, review_with_user, left_on='business_id', right_on='business_id')
    business_reviews['is_local'] = business_reviews['local_city'] == business_reviews['city']
    agg_funcs = {
        'review_id': 'count',
        'review_stars': 'mean'
    }
    grouped = business_reviews.groupby(['business_id', 'business_name', 'address', 'city', 'state', 'business_stars', 'business_review_count', 'is_local']).agg(agg_funcs).reset_index()
    return grouped

#saves the json file to specified path
def save_json(df, path):
    df.to_json(path, orient='records', lines=True)

#reads the gzip json files
#only reads 15,000 lines as the code takes too long running more.
def read_gzipped_json_lines(directory):
    gz_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.gz')]
    max_lines = 15000
    line_count = 0
    df_list = []
    for file in gz_files:
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for line in f:
                json_str_io = StringIO(line)
                df_list.append(pd.read_json(json_str_io, lines=True))
                line_count += 1
                if line_count >= max_lines:
                    break
        if line_count >= max_lines:
            break
    return pd.concat(df_list, ignore_index=True)

#gets if it is a city or state and organizes the output and then displays and saves it as a json
def display_output(business_reviews, output, location):
    if location:
        if len(location) <= 3:
            business_reviews = business_reviews[business_reviews['state'] == location]
        else:
            business_reviews = business_reviews[business_reviews['city'] == location]
    locals_higher = business_reviews[business_reviews['is_local'] == True]
    outoftown_higher = business_reviews[business_reviews['is_local'] == False]
    cols_to_drop = ['business_id', 'business_stars', 'business_review_count', 'is_local']
    locals_higher = locals_higher.drop(columns=cols_to_drop)
    outoftown_higher = outoftown_higher.drop(columns=cols_to_drop)

    locals_higher = locals_higher.rename(columns={'review_stars': 'average_stars_from_locals', 'review_id': 'reviews_from_locals'})
    outoftown_higher = outoftown_higher.rename(columns={'review_stars': 'average_stars_from_out_of_town', 'review_id': 'reviews_from_out_of_town'})

    locals_higher = locals_higher[['business_name', 'average_stars_from_locals', 'reviews_from_locals', 'city', 'state', 'address']]
    outoftown_higher = outoftown_higher[['business_name', 'average_stars_from_out_of_town', 'reviews_from_out_of_town', 'city', 'state', 'address']]

    print ("Local rating is higher dataframe")
    print (locals_higher.head(10))
    print ("Out of town rating is higher")
    print (outoftown_higher.head(10))
    save_json(locals_higher, f'{output}/locals.json')
    save_json(outoftown_higher, f'{output}/outoftown.json')

#reads in the business, user, review data files
#renames specific columns in those data files
#gets the the users reviews and location
#gets the business dataframe with local and out of town reviews and stars
#displays and saves output
def main(business_path, users_path, review_path, output_path, location):
    print ("Loading business/ data files")
    business = read_gzipped_json_lines(business_path)
    business = business.rename(columns={'name': 'business_name', 'stars': 'business_stars', 'review_count': 'business_review_count'})
    print ("Loading users/ data files")
    users = read_gzipped_json_lines(users_path)
    print ("Loading review/ data files")
    review = read_gzipped_json_lines(review_path)
    review = review.rename(columns={'stars': 'review_stars'})
    active_users = get_local_city(users, review, business)
    business_reviews = get_local_business_review(business, active_users, review)
    print ("Displaying the output:")
    display_output(business_reviews, output_path, location)

if __name__ == '__main__':
    business = sys.argv[1]
    users = sys.argv[2]
    review = sys.argv[3]
    output = sys.argv[4]
    location = sys.argv[5] if len(sys.argv) > 5 else None
    main(business, users, review, output, location)
