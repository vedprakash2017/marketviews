"""
Redis Bus - Shared state and message broker for multiprocessing
"""
import redis
import json
from src.shared.types import CleanTweet


class RedisBus:
    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initialize Redis connection.
        Each process will instantiate its own copy to avoid connection conflicts.
        """
        # decode_responses=True gives us Strings, not Bytes
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        
    def is_duplicate(self, tweet_id: str, ttl: int = 86400) -> bool:
        """
        Atomic Check-and-Set for Deduplication.
        Returns True if the ID already existed.
        
        Args:
            tweet_id: Unique identifier for the tweet
            ttl: Time to live in seconds (default 24 hours)
        
        Returns:
            bool: True if duplicate, False if new
        """
        key = f"dedup:{tweet_id}"
        # setnx returns 1 if set (new), 0 if not set (duplicate)
        is_new = self.client.setnx(key, "1")
        if is_new:
            self.client.expire(key, ttl)
            return False  # It is NOT a duplicate
        return True  # It IS a duplicate
    
    def push_to_stream(self, stream_key: str, tweet: CleanTweet):
        """
        Pushes the Pydantic model to Redis Stream
        
        Args:
            stream_key: Redis stream name (e.g., "stream:clean_tweets")
            tweet: CleanTweet object to push
        """
        # Serialize to JSON
        data = tweet.model_dump(mode='json')
        # Flatten for Redis (Redis streams prefer flat dicts of strings)
        flat_data = {
            "json_payload": json.dumps(data)
        }
        self.client.xadd(stream_key, flat_data)
        
    def get_stream_length(self, stream_key: str) -> int:
        """Get the number of items in a stream"""
        return self.client.xlen(stream_key)
    
    def read_stream(self, stream_key: str, count: int = 10):
        """Read items from a stream"""
        messages = self.client.xread({stream_key: '0-0'}, count=count)
        results = []
        for stream, items in messages:
            for msg_id, data in items:
                json_data = json.loads(data['json_payload'])
                results.append((msg_id, json_data))
        return results
    
    def ping(self) -> bool:
        """Check if Redis is connected"""
        try:
            return self.client.ping()
        except:
            return False
