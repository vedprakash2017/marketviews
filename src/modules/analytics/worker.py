"""
Analytics Worker - Consumes clean tweets and generates trading signals
Uses Redis Consumer Groups for reliable message processing
"""
import json
import time
from multiprocessing import Process

from src.shared.bus import RedisBus
from src.shared.types import CleanTweet
from src.modules.analytics.model import HybridSignalEngine
from src.shared.logger import get_logger


class AnalyticsWorker(Process):
  """
  Async worker that:
  1. Consumes clean tweets from Redis stream
  2. Runs through Hybrid Signal Engine
  3. Publishes signals to Redis pub/sub channel
  4. Logs actionable signals to terminal
  """
  
  def __init__(self, redis_config: dict):
    super().__init__()
    self.redis_config = redis_config
    self.daemon = True
    self.logger = get_logger("AnalyticsWorker", redis_config=redis_config)
  
  def run(self):
    self.logger.info("Worker Started")
    
    # 1. Initialize Resources
    bus = RedisBus(**self.redis_config)
    model = HybridSignalEngine(window_size=50, redis_config=self.redis_config)
    model.initialize() # Pre-load TF-IDF
    
    # Create Consumer Group
    try:
      bus.client.xgroup_create(
        "stream:clean_tweets",
        "analytics_group",
        id='0', # Start from beginning
        mkstream=True
      )
      self.logger.info("Created consumer group 'analytics_group'")
    except Exception:
      self.logger.info("Consumer group 'analytics_group' already exists")
    
    self.logger.info("Listening for tweets...")
    self.logger.info("Will generate BUY/SELL signals with confidence > 0.6")
    
    last_stats_time = time.time()
    
    while True:
      try:
        # 2. Consume from Redis
        messages = bus.client.xreadgroup(
          groupname="analytics_group",
          consumername="analyst_1",
          streams={"stream:clean_tweets": ">"},
          count=10,
          block=2000 # Block for 2 seconds
        )
        
        if messages:
          for stream_name, rows in messages:
            for msg_id, data in rows:
              # 3. Parse
              payload = json.loads(data['json_payload'])
              tweet = CleanTweet(**payload)
              
              # 4. Predict
              signal = model.predict(tweet)
              
              if signal:
                # 5. Publish to Redis pub/sub
                bus.client.publish(
                  "channel:live_signals",
                  signal.model_dump_json()
                )
                
                # Show ALL signals with scores
                signal_str = signal.signal.ljust(4)
                self.logger.info(f"{signal_str} | {signal.ticker:12} | Score: {signal.composite_score:+.3f} | Conf: {signal.confidence_score:.3f}")
                self.logger.info(f"Factors: {', '.join(signal.factors)}")
                
                if signal.signal != "HOLD":
                  self.logger.info(f"** {signal.signal} SIGNAL **")
              else:
                 self.logger.debug(f"Skipped tweet {tweet.tweet_id} (No signal generated)")
              
              # 7. ACK
              bus.client.xack("stream:clean_tweets", "analytics_group", msg_id)
        
        # Print stats every 30 seconds
        if time.time() - last_stats_time > 30:
          self.logger.info(f"Stats: Processed {model.total_processed}, Signals: {model.signals_generated}")
          last_stats_time = time.time()
        
      except KeyboardInterrupt:
        self.logger.info("Shutting down...")
        break
      except Exception as e:
        self.logger.error(f"Error: {e}")
        time.sleep(1)
    
    self.logger.info("Final Stats", total_processed=model.total_processed, signals_generated=model.signals_generated)


class AnalyticsManager:
  """Manages the analytics worker lifecycle"""
  
  def __init__(self, redis_config: dict):
    self.worker = AnalyticsWorker(redis_config)
  
  def start(self):
    """Start the analytics worker"""
    self.worker.start()
    return self.worker
  
  def stop(self):
    """Stop the analytics worker"""
    if self.worker.is_alive():
      self.worker.terminate()
      self.worker.join(timeout=5)
