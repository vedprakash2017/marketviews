"""
Pipeline Steps - Chain of Responsibility Pattern
Each step can transform or filter the data
"""
import re
from typing import Optional
from src.shared.types import RawTweet, CleanTweet
from src.shared.bus import RedisBus


class IPipelineStep:
    """Interface for pipeline steps"""
    def execute(self, payload) -> Optional[object]:
        raise NotImplementedError("Pipeline step must implement execute()")


class TextCleaningStep(IPipelineStep):
    """
    Step 1: CPU-heavy text cleaning
    - Remove URLs
    - Normalize currency symbols
    - Strip extra whitespace
    """
    def __init__(self):
        self.url_pattern = re.compile(r'https?://\S+|www\.\S+')
        self.currency_map = {"₹": "INR ", "$": "USD "}
        self.whitespace_pattern = re.compile(r'\s+')
        
    def execute(self, tweet: RawTweet) -> Optional[CleanTweet]:
        """
        Clean the tweet text
        
        Args:
            tweet: RawTweet object from scraper
            
        Returns:
            CleanTweet object with cleaned content, or None if invalid
        """
        from datetime import datetime
        timestamp = datetime.now().strftime('%H:%M:%S')

        # 1. CPU Heavy Regex - Remove URLs
        text = self.url_pattern.sub('', tweet.content)
        
        # 2. Replace currency symbols
        for sym, code in self.currency_map.items():
            text = text.replace(sym, code)
        
        # 3. Normalize whitespace
        text = self.whitespace_pattern.sub(' ', text)
        text = text.strip()
        
        # 4. Validation: Skip if too short after cleaning
        if len(text) < 10:
            print(f"[{timestamp}] [Processing] Dropped tweet {tweet.tweet_id}: Too short ({len(text)} chars)")
            return None
        
        # Create Clean Object
        clean = CleanTweet(
            tweet_id=tweet.tweet_id,
            content=text,
            original_content=tweet.content,
            username=tweet.username,
            timestamp=tweet.timestamp,
            metrics=tweet.metrics,
            hashtags=tweet.hashtags
        )
        return clean


class RedisDedupStep(IPipelineStep):
    """
    Step 2: I/O-bound deduplication check
    Uses Redis to check if we've seen this tweet before
    """
    def __init__(self, bus: RedisBus):
        self.bus = bus
        
    def execute(self, tweet: CleanTweet) -> Optional[CleanTweet]:
        """
        Check if tweet is duplicate
        
        Args:
            tweet: CleanTweet object
            
        Returns:
            CleanTweet if unique, None if duplicate
        """
        from datetime import datetime
        timestamp = datetime.now().strftime('%H:%M:%S')

        # I/O Bound Check
        if self.bus.is_duplicate(tweet.tweet_id):
            print(f"[{timestamp}] [Processing] Dropped tweet {tweet.tweet_id}: Duplicate")
            return None  # Stop the chain - this is a duplicate
        return tweet


class SentimentAnalysisStep(IPipelineStep):
    """
    Optional Step 3: Could add sentiment analysis here
    (Not implemented yet, just a placeholder)
    """
    def execute(self, tweet: CleanTweet) -> Optional[CleanTweet]:
        # Future: Add sentiment score to tweet
        return tweet

