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
from src.shared.log_utils import log_print


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
  
  def run(self):
    log_print("[Analytics] Worker Started")
    
    # 1. Initialize Resources
    bus = RedisBus(**self.redis_config)
    model = HybridSignalEngine(window_size=50)
    model.initialize() # Pre-load TF-IDF
    
    # Create Consumer Group
    try:
      bus.client.xgroup_create(
        "stream:clean_tweets",
        "analytics_group",
        id='0', # Start from beginning
        mkstream=True
      )
      log_print("[Analytics] Created consumer group 'analytics_group'")
    except Exception:
      log_print("[Analytics] Consumer group 'analytics_group' already exists")
    
    log_print("[Analytics] Listening for tweets...")
    log_print("[Analytics] Will generate BUY/SELL signals with confidence > 0.6")
    print("")  # Empty line for spacing
    
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
              
              # Debug: Confirm receipt
              # log_print(f"[Analytics] Processing tweet {tweet.tweet_id}...")
              
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
                log_print(f"[Analytics] {signal_str} | {signal.ticker:12} | Score: {signal.composite_score:+.3f} | Conf: {signal.confidence_score:.3f}")
                log_print(f"            Factors: {', '.join(signal.factors)}")
                
                if signal.signal != "HOLD":
                  log_print(f"            ** {signal.signal} SIGNAL **")
                  print("")
              else:
                 log_print(f"[Analytics] Skipped tweet {tweet.tweet_id} (No signal generated)")
              
              # 7. ACK
              bus.client.xack("stream:clean_tweets", "analytics_group", msg_id)
        
        # Print stats every 30 seconds
        if time.time() - last_stats_time > 30:
          log_print(f"[Analytics] Stats: Processed {model.total_processed}, Signals: {model.signals_generated}")
          last_stats_time = time.time()
        
      except KeyboardInterrupt:
        log_print("\n[Analytics] Shutting down...")
        break
      except Exception as e:
        log_print(f"[Analytics] Error: {e}")
        time.sleep(1)
    
    log_print(f"[Analytics] Final Stats:")
    log_print(f" Total processed: {model.total_processed}")
    log_print(f" Signals generated: {model.signals_generated}")


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
