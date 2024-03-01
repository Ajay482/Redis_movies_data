import matplotlib.pyplot as plt
from db_config import get_redis_connection

class GenreVisualizer:
    """Class to handle interaction with Redis for storing and retrieving JSON data."""

    def __init__(self, redis_connection):
        """
        Initialize RedisHandler with a Redis connection.

        Args:
            redis_connection: Redis connection object.
        """
        self.redis_connection = redis_connection

    def get_genre_count(self):
        """
        Count the occurrences of each genre in the movie data stored in Redis.

        Returns:
            dict: A dictionary containing the count of each genre.
        """
        genre_cnt = {}
        
        cursor = b'0'  # Initialize cursor

        while True:
            cursor, keys = self.redis_connection.scan(cursor, match='data:movies.*')
            for key in keys:
                genre_len = self.redis_connection.execute_command('JSON.ARRLEN', key, '.genre')
                for j in range(genre_len):
                    genre = self.redis_connection.execute_command('JSON.GET', key, f'.genre.[{j}]')
                    genre = genre.strip('""')
                    genre_cnt[genre] = genre_cnt.get(genre, 0) + 1
            if cursor == 0:
                break
        print(genre_cnt)
        return genre_cnt

        
    def visualise_genre(self, genre_count):
        """
        Visualize the data using matplotlib.

        Args:
            genre_count: Dictionary containing genre counts.
        """
        plt.figure(figsize=(8, 6))
        plt.barh(list(genre_count.keys()), list(genre_count.values()), color='skyblue')
        
        plt.xlabel('Count', fontsize=14)
        plt.ylabel('Genre', fontsize=14)
        plt.title('Genre Count', fontsize=16)
        
        for i, count in enumerate(genre_count.values()):
            plt.text(count, i, str(count), ha='left', va='center', fontsize=12)

        plt.tight_layout()

        plt.show()


if __name__ == "__main__":
    redis_connection = get_redis_connection()
    genre_visualizer = GenreVisualizer(redis_connection)
    genre_cnt = genre_visualizer.get_genre_count()
    genre_visualizer.visualise_genre(genre_cnt)
