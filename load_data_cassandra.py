import sys
import os
import gzip
import json
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

#creates session
#writes business, user, and review data into cassandra
def main(business_path, users_path, review_path, keyspace):
    session = create_session(keyspace)
    process_directory(session, business_path, 'business')
    process_directory(session, users_path, 'user')
    process_directory(session, review_path, 'review')

#writes the dataset we called into Cassandra
def process_directory(session, directory_path, table_name):
    for f in os.listdir(directory_path):
        with gzip.open(os.path.join(directory_path, f), 'rt', encoding='utf-8') as datafile:
            data_to_insert = []
            for line in datafile:
                data = json.loads(line)
                if table_name == 'business':
                    record = (data['business_id'], data['name'], data['address'],
                              data['city'], data['state'], data['stars'], data['review_count'])
                elif table_name == 'review':
                    record = (data['review_id'], data['user_id'], data['business_id'], data['stars'])
                elif table_name == 'user' and data['review_count'] > 5:
                    record = (data['user_id'], data['name'], data['review_count'])
                else:
                    continue
                data_to_insert.append(record)
            insert_data(session, table_name, data_to_insert)

#writes data into tables in Cassandra with a batch size of 200
def insert_data(session, table_name, data):
    if table_name == 'business':
        query = "INSERT INTO business (business_id, name, address, city, state, stars, review_count) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    elif table_name == 'review':
        query = "INSERT INTO review (review_id, user_id, business_id, stars) VALUES (%s, %s, %s, %s)"
    elif table_name == 'user':
        query = "INSERT INTO user (user_id, name, review_count) VALUES (%s, %s, %s)"

    batch = BatchStatement()
    batch_size = 200
    for record in data:
        batch.add(SimpleStatement(query), record)
        if len(batch) >= batch_size:
            session.execute(batch)
            batch.clear()
    if len(batch) > 0:
        session.execute(batch)

#creates cluster session
def create_session(keyspace):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    return session

if __name__ == '__main__':
    business_path = sys.argv[1]
    users_path = sys.argv[2]
    review_path = sys.argv[3]
    keyspace = sys.argv[4]
    main(business_path, users_path, review_path, keyspace)
