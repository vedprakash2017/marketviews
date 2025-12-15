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
from src.shared.log_utils import log_print


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
    
  def run(self):
    """
    This code runs in a separate OS Process.
    It shares NOTHING with the main process.
    """
    log_print(f"[Processor-{self.worker_id}] Starting on PID {os.getpid()}")
    
    # 1. Initialize Local Resources (Cannot be shared across processes)
    bus = RedisBus(**self.redis_config)
    
    # 2. Build Pipeline
    pipeline = ProcessingPipeline(steps=[
      TextCleaningStep(),
      RedisDedupStep(bus)
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
            log_print(f"[Processor-{self.worker_id}] Processed {processed_count} tweets")
        else:
          # Filtered out (duplicate or invalid)
          duplicate_count += 1
          
      except Exception as e:
        # Queue.Empty is normal - workers poll every second
        # Only print actual errors, not queue timeouts
        if e.__class__.__name__ != 'Empty':
          error_count += 1
          log_print(f"[Processor-{self.worker_id}] Error: {e}")
        continue


class ProcessingManager:
  """
  Manages multiple ProcessingWorker processes
  """
  def __init__(self, input_queue: Queue):
    self.input_queue = input_queue
    self.workers = []
    
  def start(self, workers_count=None):
    """
    Spawn worker processes
    
    Args:
      workers_count: Number of workers (default: CPU cores - 2)
    """
    # Use all cores except 2 (one for Scraper, one for OS)
    if not workers_count:
      workers_count = max(1, cpu_count() - 2)
      
    log_print(f"[ProcessingManager] Spawning {workers_count} Processing Workers...")
    
    config = {'host': 'localhost', 'port': 6379}
    
    for i in range(workers_count):
      worker = ProcessingWorker(i, self.input_queue, config)
      worker.start()
      self.workers.append(worker)
      
    log_print(f"[ProcessingManager] All workers started!")
    
  def stop(self):
    """Stop all workers"""
    log_print("Stopping...")
    for worker in self.workers:
      worker.terminate()
      worker.join(timeout=5)
    log_print(f"[ProcessingManager] All workers stopped!")
    
  def is_alive(self) -> bool:
    """Check if any workers are still running"""
    return any(worker.is_alive() for worker in self.workers)

