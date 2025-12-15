from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict
from datetime import datetime
import pandas as pd

@dataclass
class RawTweet:
    content: str
    source_id: str
    timestamp: float
    author: str
    url: str

class IDataSource(ABC):
    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def fetch_latest(self, query: str, limit: int = 10) -> List[RawTweet]:
        pass

    @abstractmethod
    async def close(self):
        pass


class IDataRepository(ABC):
    """Interface for data persistence"""
    
    @abstractmethod
    def save_batch(self, tweets: List):
        """Save a batch of tweets to persistent storage"""
        pass
    
    @abstractmethod
    def load_range(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Load tweets within a time range"""
        pass
