"""
Shared data types for the entire pipeline
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, List
from datetime import datetime


class TweetMetrics(BaseModel):
    """Tweet engagement metrics"""
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    views: int = 0


class RawTweet(BaseModel):
    """Raw tweet data from scraper"""
    tweet_id: str
    content: str
    username: str
    timestamp: float
    url: str
    metrics: Optional[TweetMetrics] = None
    hashtags: List[str] = Field(default_factory=list)


class CleanTweet(BaseModel):
    """Cleaned and processed tweet"""
    tweet_id: str
    content: str  # Cleaned content
    original_content: str  # Original for reference
    username: str
    timestamp: float
    metrics: Optional[TweetMetrics] = None
    hashtags: List[str] = Field(default_factory=list)
    processed_at: float = Field(default_factory=lambda: datetime.now().timestamp())
