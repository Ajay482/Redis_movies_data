import matplotlib.pyplot as plt
from db_config import get_redis_connection

class MovieDataHandler:
    """Class to handle interaction with Redis for storing and retrieving JSON data."""

    def __init__(self, redis_connection):
        """
        Initialize MovieDataHandler with a Redis connection.

        Args:
            redis_connection: Redis connection object.
        """
        self.redis_connection = redis_connection


    def get_top_movies_and_ratings(self, n=15):
        """
        Retrieve top movies and their IMDb ratings from Redis.

        Args:
            n (int): Number of top movies to retrieve. Default is 15.

        Returns:
            Tuple: Tuple containing lists of top movie titles and ratings.
        """
        cursor = b'0'  # Initialize cursor
        movies = []
        ratings = []

        while True:
            cursor, keys = self.redis_connection.scan(cursor, match='data:movies.*')
            for key in keys:
                movie_title = self.redis_connection.execute_command('JSON.GET', key, '.title')
                imdb_rating = self.redis_connection.execute_command('JSON.GET', key, 'imdbrating')
                movies.append(movie_title.strip('""'))
                ratings.append(float(imdb_rating))

            if cursor == 0:
                break

        sorted_movies, sorted_ratings = zip(*sorted(zip(movies, ratings), key=lambda x: x[1], reverse=True))

        return sorted_movies[:n], sorted_ratings[:n]


    def visualize_top_ratings(self, movies, ratings):
        """
        Visualize IMDb ratings for top movies using a bar plot.

        Args:
            movies (list): List of movie titles.
            ratings (list): List of IMDb ratings.
        """
        plt.figure(figsize=(14, 8))
        plt.barh(movies, ratings, color='skyblue')
        plt.xlabel('IMDb Rating', fontsize=14)
        plt.ylabel('Movie', fontsize=14)
        plt.title('Top 15 IMDb Ratings by Movie', fontsize=16)
        plt.gca().invert_yaxis()
        
        for i, rating in enumerate(ratings):
            plt.text(rating, i, f'{rating:.1f}', ha='left', va='center', fontsize=10)
        
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.show()


if __name__ == "__main__":
    redis_connection = get_redis_connection()
    movie_handler = MovieDataHandler(redis_connection)
    top_movies, top_ratings = movie_handler.get_top_movies_and_ratings(n=15)
    movie_handler.visualize_top_ratings(top_movies, top_ratings)
