import json
import requests
from db_config import get_redis_connection

class OTTSearchAndRedisHandler:
    """
    Class to handle interaction with Redis for storing and retrieving JSON data,
    and to interact with the OTT details API for advanced search.
    """

    def __init__(self, redis_connection):
        """
        Initialize OTTSearchAndRedisHandler with API key, host, and Redis connection.

        Args:
            api_key (str): API key for accessing the API.
            api_host (str): Host URL for the API.
            redis_connection: Redis connection object.
        """
        self.api_key = "ea341da3eamsh8f7ddd838e045b2p1b1727jsn78e845e3b3f8"
        self.api_host = "ott-details.p.rapidapi.com"
        self.redis_connection = redis_connection

    def insert_search_results_into_redis(self, search_results):
        """
        Insert search results JSON data into Redis.

        Args:
            search_results: JSON data containing search results.
        """
        self.redis_connection.delete('data:movies')
        for i, movie in enumerate(search_results):
            json_data = json.dumps(movie)
            key = f'data:movies:{i}'
            self.redis_connection.execute_command('JSON.SET', key, '.', json_data)

    def advanced_search_and_store_in_redis(self, start_year, end_year, content_type, min_imdb, max_imdb, language):
        """
        Perform an advanced search on the OTT details API and store the results in Redis.

        Args:
            start_year (int): Start year for filtering content.
            end_year (int): End year for filtering content.
            content_type (str): Type of content (e.g., 'movie', 'series').
            min_imdb (float): Minimum IMDb rating.
            max_imdb (float): Maximum IMDb rating.
            language (str): Language of the content.
        """
        url = "https://ott-details.p.rapidapi.com/advancedsearch"
        querystring = {
            "start_year": start_year,
            "end_year": end_year,
            "type": content_type,
            "min_imdb": min_imdb,
            "max_imdb": max_imdb,
            "language": language
        }
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
        response = requests.get(url, headers=headers, params=querystring)
        search_results = response.json()['results']
        print(f"Number of search results: {len(search_results)}")  # Print the length of search_results
        self.insert_search_results_into_redis(search_results)


if __name__ == "__main__":
    redis_connection = get_redis_connection()
    
    handler = OTTSearchAndRedisHandler(redis_connection)
    handler.advanced_search_and_store_in_redis(2019, 2023, "movie", 5, 9, "english")
