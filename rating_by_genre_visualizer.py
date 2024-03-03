import json
import matplotlib.pyplot as plt
import numpy as np
from db_config import get_redis_connection

class GenreRatingsHandler:
    """Class to handle interaction with Redis for storing and retrieving JSON data."""

    def __init__(self, redis_connection):
        """
        Initialize GenreRatingsHandler with a Redis connection.

        Args:
            redis_connection: Redis connection object.
        """
        self.redis_connection = redis_connection
    
    
    def get_ratings_by_genre(self):
        """
        Retrieve IMDb ratings for each genre from Redis.

        Returns:
            dict: A dictionary containing lists of IMDb ratings for each genre.
        """
        ratings_by_genre = {}
        cursor = b'0'
        
        while True:
            cursor, keys = self.redis_connection.scan(cursor, match='data:movies:*')
            for key in keys:
                movie_data = self.redis_connection.execute_command('JSON.GET', key)
                movie_data = json.loads(movie_data)
                
                for genre in movie_data.get('genre', []):
                    genre = genre.strip('""')
                    rating = float(movie_data.get('imdbrating', 0))
                    
                    if genre not in ratings_by_genre:
                        ratings_by_genre[genre] = [rating]
                    else:
                        ratings_by_genre[genre].append(rating)
            
            if cursor == 0:
                break
                
        return ratings_by_genre
       
        
    def visualise_ratings_by_genre(self, ratings_by_genre):
        """
        Visualize the IMDb ratings distribution by genre using a box plot.

        Args:
            ratings_by_genre (dict): A dictionary containing lists of IMDb ratings for each genre.
        """
        mean_ratings = {genre: np.median(ratings) for genre, ratings in ratings_by_genre.items()}
        ratings_lists = list(ratings_by_genre.values())
        
        plt.figure(figsize=(12, 8))
        plt.boxplot(ratings_lists, labels=list(ratings_by_genre.keys()))

        for i, (genre, mean_rating) in enumerate(mean_ratings.items()):
            plt.text(i + 1, mean_rating, f'{mean_rating:.2f}', ha='center', va='bottom', fontsize=10, color='red')

        plt.xlabel('Genre', fontsize=14)
        plt.ylabel('IMDb Rating', fontsize=14)
        plt.title('IMDb Ratings Distribution by Genre', fontsize=16)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    redis_connection = get_redis_connection()
    handler = GenreRatingsHandler(redis_connection)
    ratings_by_genre = handler.get_ratings_by_genre()
    handler.visualise_ratings_by_genre(ratings_by_genre)
