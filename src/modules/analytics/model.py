"""
Hybrid Signal Engine - Combines Text, User, and Engagement signals
Uses TF-IDF for importance weighting and statistical confidence scoring
"""
import numpy as np
from collections import deque
from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List, Dict, Optional, Tuple
from pydantic import BaseModel

from src.shared.types import CleanTweet
from config.influencers import CATEGORICAL_TAGS


class TradeSignal(BaseModel):
    """Trading signal output"""
    timestamp: float
    ticker: str
    signal: str
    composite_score: float
    confidence_score: float
    factors: List[str]
    tweet_content: str = ""


class HybridSignalEngine:
    """Generates trading signals from tweet analysis"""
    
    def __init__(self, window_size: int = 50):
        self.vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
        
        self.lexicon = {
            'breakout': 1.0, 'green': 0.8, 'buy': 0.9, 'support': 0.7,
            'upper': 0.9, 'long': 0.7, 'bullish': 1.0, 'rally': 0.8,
            'surge': 0.9, 'gain': 0.7, 'profit': 0.6, 'high': 0.7,
            'strong': 0.6, 'positive': 0.7, 'circuit': 0.9, 'target': 0.6,
            'upar': 0.8, 'tezi': 0.8, 'badhega': 0.7, 'lelo': 0.6,
            'kharido': 0.7, 'badiya': 0.6, 'mauka': 0.7,
            'breakdown': -1.0, 'red': -0.8, 'sell': -0.9, 'resistance': -0.7,
            'lower': -0.9, 'short': -0.7, 'bearish': -1.0, 'crash': -1.0,
            'fall': -0.8, 'drop': -0.8, 'loss': -0.7, 'low': -0.7,
            'weak': -0.6, 'negative': -0.7, 'dump': -0.9, 'exit': -0.6,
            'niche': -0.8, 'mandi': -0.8, 'girega': -0.7, 'becho': -0.6,
            'khatam': -0.8, 'duba': -0.9, 'nikalo': -0.7
        }
        
        self.user_tiers = {
            'verified_news': 1.0,
            'influencer': 0.7,
            'default': 0.3
        }
        
        self.windows: Dict[str, deque] = {}
        self.window_size = window_size
        self.is_fitted = False
        self.total_processed = 0
        self.signals_generated = 0
    
    def initialize(self):
        print("[Analytics] Initializing signal engine...")
        
        seed_corpus = [
            "market breakout nifty banknifty high upper circuit",
            "gap down opening massive selling pressure crash",
            "support level strong buying interest bullish",
            "resistance broken bearish trend short position",
            "reliance results positive long build up rally",
            "inflation data bad for markets correction expected",
            "nifty50 index green candles breakout level",
            "bank nifty red candles breakdown support",
            "sensex rally strong gains positive sentiment",
            "market crash fear selling pressure weak"
        ]
        
        self.vectorizer.fit(seed_corpus)
        self.is_fitted = True
        print(f"[Analytics] Vocab: {len(self.vectorizer.vocabulary_)}, Lexicon: {len(self.lexicon)}, Sources: {len(CATEGORICAL_TAGS['verified_news']) + len(CATEGORICAL_TAGS['influencer'])}")
    
    def _get_user_score(self, username: str) -> Tuple[float, str]:
        u = username.lower().replace('@', '').strip()
        
        for news in CATEGORICAL_TAGS['verified_news']:
            if news in u:
                return self.user_tiers['verified_news'], "News"
        
        for inf in CATEGORICAL_TAGS['influencer']:
            if inf in u:
                return self.user_tiers['influencer'], "Influencer"
        
        return self.user_tiers['default'], "Random"
    
    def predict(self, tweet: CleanTweet) -> Optional[TradeSignal]:
        if not self.is_fitted:
            return None
        
        self.total_processed += 1
        factors = []
        
        # Text score
        words = tweet.content.lower().split()
        text_raw = 0.0
        matched_words = []
        
        for word in words:
            if word in self.lexicon:
                direction = self.lexicon[word]
                
                try:
                    word_idx = self.vectorizer.vocabulary_.get(word)
                    if word_idx is not None:
                        idf = self.vectorizer.idf_[word_idx]
                    else:
                        idf = 1.0
                except:
                    idf = 1.0
                
                text_raw += direction * idf
                matched_words.append(word)
        
        text_score = max(-1.0, min(1.0, text_raw / 5.0))
        factors.append(f"Text: {text_score:.2f} ({len(matched_words)} words)")
        
        # User score
        user_score, user_type = self._get_user_score(tweet.username)
        factors.append(f"Source: {user_type} ({user_score:.1f})")
        
        # Engagement score
        likes = tweet.metrics.likes if tweet.metrics else 0
        eng_score = min(1.0, np.log10(likes + 1) / 4.0)
        factors.append(f"Viral: {eng_score:.2f} ({likes} likes)")
        
        # Composite calculation
        direction_sign = 1.0 if text_score >= 0 else -1.0
        
        composite = direction_sign * (
            (0.5 * abs(text_score)) +
            (0.3 * user_score) +
            (0.2 * eng_score)
        )
        
        # Statistical aggregation
        ticker = "nifty50"
        if tweet.hashtags:
            ticker = tweet.hashtags[0].lower()
        
        if ticker not in self.windows:
            self.windows[ticker] = deque(maxlen=self.window_size)
        
        self.windows[ticker].append(composite)
        
        if len(self.windows[ticker]) < 5:
            return None
        
        data = np.array(self.windows[ticker])
        mean_score = float(np.mean(data))
        std_dev = float(np.std(data))
        confidence = max(0.0, 1.0 - std_dev)
        
        # Signal generation
        signal = "HOLD"
        
        if mean_score > 0.25 and confidence > 0.6:
            signal = "BUY"
            self.signals_generated += 1
        elif mean_score < -0.25 and confidence > 0.6:
            signal = "SELL"
            self.signals_generated += 1
        
        return TradeSignal(
            timestamp=tweet.timestamp,
            ticker=ticker.upper(),
            signal=signal,
            composite_score=mean_score,
            confidence_score=confidence,
            factors=factors,
            tweet_content=tweet.content[:80]
        )
