import json
from decimal import *
import config
import dynamodb_client

def main():
    recommendation_queue = config.sqs.get_queue_by_name(QueueName=config.recommendation_queue_name)
    process_messages(recommendation_queue)

def process_messages(recommendation_queue):
    for message in recommendation_queue.receive_messages(MaxNumberOfMessages=config.max_queue_messages):
        print("SQS Message: {}".format(message.body))
        rating = json.loads(message.body)
        dynamodb_client.update_restaurant(rating['restaurant'])
        update_recommendations(rating)
        message.delete()
        
def update_recommendations(rating):
    user_id = rating['user-id']
    restaurant_location = rating['restaurant']['restaurant-location']
    print('Updating recommendations for user: {}'.format(user_id))
    user_ratings_restaurant_id_list = []
    near_restaurant_id_list = []
    # Get the current recommendations for the user
    recommendation_map = dynamodb_client.get_recommendation_map_by_user_id(user_id)['Items'] 
    if len(recommendation_map) > 0:
        recommendation_map = recommendation_map[0]['recommendation-map']
    else:
        recommendation_map = {}
    # Get the similarity index map for the user
    user_similarity_index_map = dynamodb_client.get_similarity_index_map_by_user_id(user_id)['Items']
    if len(user_similarity_index_map) > 0:
        user_similarity_index_map = user_similarity_index_map[0]['similarity-index-map']
    # Find the restaurants near by that the user has not rated
    user_ratings = dynamodb_client.get_all_ratings_by_user_id(user_id)['Items']
    for u_rating in user_ratings:
        user_ratings_restaurant_id_list.append(u_rating['restaurant-id'])
    near_restaurants = dynamodb_client.get_near_restaurants_by_lat_long(restaurant_location['lat'], restaurant_location['lng'])['Items']
    for restaurant in near_restaurants:
        near_restaurant_id_list.append(restaurant['restaurant-id'])
    restaurants_not_rated = set(user_ratings_restaurant_id_list)^set(near_restaurant_id_list)
    # For each restaurant not rated, compute the probability the user will like the restaurant
    for restaurant_id in restaurants_not_rated:
        r_ratings = dynamodb_client.get_all_ratings_by_restaurant_id(restaurant_id)['Items']
        probability_numerator = 0
        probability_denominator = len(r_ratings)
        for rating in r_ratings:
            rating_user = rating['user-id']
            rating_value = rating['rating-value']
            rating_restaurant = rating['restaurant-id']
            if rating_user in user_similarity_index_map:
                probability_numerator = probability_numerator + user_similarity_index_map[rating_user] \
                    if rating_value > 0 else probability_numerator - user_similarity_index_map[rating_user]
            recommendation_map[rating_restaurant] = Decimal(probability_numerator) / Decimal(probability_denominator) 
    # Update DynamoDB with the recommendation map for the user
    dynamodb_client.update_user_recommendation_map(user_id, recommendation_map)

if __name__ == "__main__":
    main()
