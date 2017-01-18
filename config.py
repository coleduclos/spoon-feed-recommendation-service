import boto3

aws_region = 'us-west-2'

max_queue_messages = 10
sqs = boto3.resource('sqs', region_name=aws_region)

recommendation_queue_name = 'duclos-app-recommendation-queue'

dynamodb = boto3.resource('dynamodb', region_name=aws_region)

user_ratings_dynamodb_table_name = 'duclos-app-user-ratings'
user_ratings_dynamodb_table = dynamodb.Table(user_ratings_dynamodb_table_name)
user_id_rating_value_index = 'user-id-rating-value-index'
user_id_rating_value_index_pkey = 'user-id'

restaurant_id_rating_value_index = 'restaurant-id-rating-value-index'
restaurant_id_rating_value_index_pkey = 'restaurant-id'

restaurants_dynamodb_table_name = 'duclos-app-restaurants'
restaurants_dynamodb_table = dynamodb.Table(restaurants_dynamodb_table_name)
restaurants_pkey = 'geohash'
restaurants_skey = 'restaurant-id'
restaurants_dynamodb_geohash_precision = 6

similarity_queue_name = 'duclos-app-similarity-queue'
similar_users_dynamodb_table_name = 'duclos-app-similar-users'
similar_users_dynamodb_table = dynamodb.Table(similar_users_dynamodb_table_name)
similar_users_pkey = 'user-id'
similarity_index_map_attribute = 'similarity-index-map'

recommendations_dynamodb_table_name = 'duclos-app-recommendations'
recommendations_dynamodb_table = dynamodb.Table(recommendations_dynamodb_table_name)
recommendations_pkey = 'user-id'
recommendations_skey = 'geohash'
recommendation_map_attribute = 'recommendation-map'
