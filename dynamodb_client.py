import Geohash
import config
import decimal

def get_all_ratings_by_user_id (user_id):
    print('Querying DynamoDB for all ratings from user: {} in table {}'
        .format(user_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.user_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.user_id_rating_value_index_pkey 
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : user_id
        }
    )
    return response

def get_all_ratings_by_restaurant_id (restaurant_id):
    print('Querying DynamoDB for all ratings of restaurant: {} in table {}'
        .format(restaurant_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.restaurant_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.restaurant_id_rating_value_index_pkey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : restaurant_id
        }
    )
    return response

def get_ratings_attribute_by_restaurant_id (restaurant_id, attribute):
    print('Querying DynamoDB for ratings attribute: {} of restaurant: {} in table {}'
        .format(attribute, restaurant_id, config.user_ratings_dynamodb_table_name))
    response = config.user_ratings_dynamodb_table.query(
        IndexName=config.restaurant_id_rating_value_index,
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ProjectionExpression='#attribute', 
        ExpressionAttributeNames={
            '#partitionkey' : config.restaurant_id_rating_value_index_pkey,
            '#attribute' : attribute
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : restaurant_id
        }
    )
    return response

def get_similarity_index_map_by_user_id (user_id):
    print('Querying DynamoDB for the similiarity index map of user: {} in table {}'
        .format(user_id, config.similar_users_dynamodb_table_name))
    response = config.similar_users_dynamodb_table.query(
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.similar_users_pkey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : user_id
        }
    )
    return response

def update_user_similarity_index_map (user_id, similarity_index_map):
    print('Updating DynamoDB with similarity index map for  user: {} in table {}'
        .format(user_id, config.similar_users_dynamodb_table_name))
    response = config.similar_users_dynamodb_table.update_item(
        Key={
            config.similar_users_pkey : user_id
        },
        UpdateExpression='SET #attribute = :val',
        ExpressionAttributeNames={
            '#attribute' : config.similarity_index_map_attribute
        },
        ExpressionAttributeValues={
            ':val' : similarity_index_map
        }
    )

def get_recommendation_map_by_lat_long_and_user_id (latitude, longitude, user_id):
    print('Querying DynamoDB for the recommendation map of user: {} in table {}'
        .format(user_id, config.recommendations_dynamodb_table_name))
    geohash = Geohash.encode(latitude, longitude, precision=config.restaurants_dynamodb_geohash_precision)
    response = config.recommendations_dynamodb_table.query(
        KeyConditionExpression='#partitionkey = :partitionkeyval AND #sortkey = :sortkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.recommendations_pkey,
            '#sortkey' : config.recommendations_skey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : geohash,
            ':sortkeyval' : user_id
        }
    )
    return response

def update_user_recommendation_map (latitude, longitude, user_id, recommendation_map):
    print('Updating DynamoDB with recommendation map for  user: {} in table {}'
        .format(user_id, config.recommendations_dynamodb_table_name))
    geohash = Geohash.encode(latitude, longitude, precision=config.restaurants_dynamodb_geohash_precision)
    response = config.recommendations_dynamodb_table.update_item(
        Key={
            config.recommendations_pkey : geohash,
            config.recommendations_skey : user_id
        },
        UpdateExpression='SET #attribute = :val',
        ExpressionAttributeNames={
            '#attribute' : config.recommendation_map_attribute
        },
        ExpressionAttributeValues={
            ':val' : recommendation_map
        }
    )

def get_near_restaurants_by_lat_long (latitude, longitude):
    geohash = Geohash.encode(latitude, longitude, precision=config.restaurants_dynamodb_geohash_precision)
    print('Querying DynamoDB for all restaurants with geohash: {} in table {}'
        .format(geohash, config.restaurants_dynamodb_table_name))
    response = config.restaurants_dynamodb_table.query(
        KeyConditionExpression='#partitionkey = :partitionkeyval',
        ExpressionAttributeNames={
            '#partitionkey' : config.restaurants_pkey
        },
        ExpressionAttributeValues={
            ':partitionkeyval' : geohash 
        }
    )
    return response

def update_restaurant(restaurant):
    print('Updating DynamoDB with restaurant {} in table {}'
        .format(restaurant['restaurant-id'], config.restaurants_dynamodb_table_name))

    geohash = Geohash.encode(restaurant['restaurant-location']['lat'], restaurant['restaurant-location']['lng'], 
        precision=config.restaurants_dynamodb_geohash_precision)

    print('DYNAMODB: Adding restaurant: {} ({})'.format(restaurant['restaurant-id'], geohash))

    response = config.restaurants_dynamodb_table.update_item(
        Key={
            config.restaurants_pkey: geohash,
            config.restaurants_skey: restaurant['restaurant-id']
        },
        UpdateExpression="SET #restaurant_name = :restaurant_name",
        ExpressionAttributeNames={
            '#restaurant_name' : 'restaurant-name'
        },
        ExpressionAttributeValues={
            ':restaurant_name' : restaurant['restaurant-name']
        },
        ReturnValues="UPDATED_NEW"
    )

    return response

