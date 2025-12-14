from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict

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
