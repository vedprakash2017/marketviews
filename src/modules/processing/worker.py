"""
Multiprocessing Worker - Runs in separate OS processes for true parallelism
"""
import os
import time
from multiprocessing import Process, Queue, cpu_count
from src.shared.types import RawTweet
from src.shared.bus import RedisBus
from src.modules.processing.pipeline import ProcessingPipeline
from src.modules.processing.steps import TextCleaningStep, RedisDedupStep
from src.shared.logger import get_logger


class ProcessingWorker(Process):
  """
  A worker process that:
  1. Pulls RawTweets from a Queue
  2. Runs them through the processing pipeline
  3. Pushes CleanTweets to Redis Stream
  """
  def __init__(self, worker_id: int, input_queue: Queue, redis_config: dict):
    super().__init__()
    self.worker_id = worker_id
    self.input_queue = input_queue
    self.redis_config = redis_config
    self.daemon = True # Kill process if main app dies
    self.logger = get_logger(f"ProcessingWorker-{worker_id}", redis_config=redis_config)
    
  def run(self):
    """
    This code runs in a separate OS Process.
    It shares NOTHING with the main process.
    """
    self.logger.info(f"Starting on PID {os.getpid()}")
    
    # 1. Initialize Local Resources (Cannot be shared across processes)
    bus = RedisBus(**self.redis_config)
    
    # 2. Build Pipeline
    pipeline = ProcessingPipeline(steps=[
      TextCleaningStep(redis_config=self.redis_config),
      RedisDedupStep(bus, redis_config=self.redis_config)
    ])
    
    # 3. Consumption Loop
    processed_count = 0
    duplicate_count = 0
    error_count = 0
    
    while True:
      try:
        # Blocking Get (Wait 1s)
        raw_tweet: RawTweet = self.input_queue.get(timeout=1.0)
        
        # Execute Pipeline
        result = pipeline.run(raw_tweet)
        
        if result:
          # Success: Push to Redis
          bus.push_to_stream("stream:clean_tweets", result)
          processed_count += 1
          if processed_count % 10 == 0:
            self.logger.info(f"Processed {processed_count} tweets")
        else:
          # Filtered out (duplicate or invalid)
          duplicate_count += 1
          
      except Exception as e:
        # Queue.Empty is normal - workers poll every second
        # Only print actual errors, not queue timeouts
        if e.__class__.__name__ != 'Empty':
          error_count += 1
          self.logger.error(f"Error: {e}")
        continue


class ProcessingManager:
  """
  Manages multiple ProcessingWorker processes
  """
  def __init__(self, input_queue: Queue, redis_config: dict = None):
    self.input_queue = input_queue
    self.workers = []
    self.redis_config = redis_config or {'host': 'localhost', 'port': 6379}
    self.logger = get_logger("ProcessingManager", redis_config=self.redis_config)
    
  def start(self, workers_count=None):
    """
    Spawn worker processes
    
    Args:
      workers_count: Number of workers (default: CPU cores - 2)
    """
    # Use all cores except 2 (one for Scraper, one for OS)
    if not workers_count:
      workers_count = max(1, cpu_count() - 2)
      
    self.logger.info(f"Spawning {workers_count} Processing Workers...")
    
    for i in range(workers_count):
      worker = ProcessingWorker(i, self.input_queue, self.redis_config)
      worker.start()
      self.workers.append(worker)
      
    self.logger.info("All workers started!")
    
  def stop(self):
    """Stop all workers"""
    self.logger.info("Stopping...")
    for worker in self.workers:
      worker.terminate()
      worker.join(timeout=5)
    self.logger.info("All workers stopped!")
    
  def is_alive(self) -> bool:
    """Check if any workers are still running"""
    return any(worker.is_alive() for worker in self.workers)

