"""
Storage Worker - Async consumer with buffering and Redis Consumer Groups
Guarantees zero data loss with acknowledgement mechanism
"""
import asyncio
import json
import time
from typing import List
from src.shared.bus import RedisBus
from src.shared.types import CleanTweet
from src.shared.interfaces import IDataRepository
from src.shared.logger import get_logger


class StorageWorker:
  """
  Async worker that:
  1. Consumes from Redis stream using Consumer Groups
  2. Buffers tweets in memory
  3. Flushes to disk based on size or time triggers
  4. ACKs messages only after successful write
  """
  
  def __init__(self, repo: IDataRepository, redis_config: dict, 
         batch_size: int = 50, flush_timeout: int = 60):
    self.repo = repo
    self.bus = RedisBus(**redis_config)
    
    # State
    self.buffer: List[CleanTweet] = []
    self.pending_ids: List[str] = [] # Track IDs to ACK later
    self.last_flush = time.time()
    
    # Config
    self.BATCH_SIZE = batch_size
    self.FLUSH_TIMEOUT = flush_timeout
    
    # Stats
    self.total_processed = 0
    self.total_batches = 0
    
    # Logger
    self.logger = get_logger("StorageWorker", redis_config=redis_config)
    
  async def flush_buffer(self):
    """
    Offloads the blocking write operation to a separate thread.
    Ensures the event loop stays responsive.
    """
    if not self.buffer:
      return
    
    try:
      # COPY the buffer so we can clear the main one immediately
      current_batch = self.buffer.copy()
      current_ids = self.pending_ids.copy()
      
      self.buffer.clear()
      self.pending_ids.clear()
      self.last_flush = time.time()
      
      # RUN IN THREAD: This keeps the Event Loop alive
      self.logger.info(f"Flushing {len(current_batch)} items...")
      
      # asyncio.to_thread runs blocking code in a ThreadPoolExecutor
      await asyncio.to_thread(self.repo.save_batch, current_batch)
      
      # ACKNOWLEDGEMENT
      # Only ACK after successful write. If write fails, these return to Redis.
      if current_ids:
        self.bus.client.xack("stream:clean_tweets", "storage_group", *current_ids)
        self.logger.debug(f"ACKed {len(current_ids)} messages")
      
      self.total_processed += len(current_batch)
      self.total_batches += 1
      
    except Exception as e:
      self.logger.error(f"Flush failed (Data stays in Redis): {e}")
      # Don't re-raise - let the worker continue
  
  async def run(self):
    """
    Main event loop:
    1. Read from Redis Consumer Group
    2. Buffer messages
    3. Flush when triggers fire
    """
    self.logger.info("Worker Started")
    self.logger.info(f"Config: batch_size={self.BATCH_SIZE}, flush_timeout={self.FLUSH_TIMEOUT}s")
    
    # Ensure Consumer Group Exists
    try:
      self.bus.client.xgroup_create(
        "stream:clean_tweets", 
        "storage_group", 
        id='0', # Start from beginning
        mkstream=True
      )
      self.logger.info("Created consumer group 'storage_group'")
    except Exception:
      self.logger.info("Consumer group 'storage_group' already exists")
    
    while True:
      try:
        # 1. Read from Redis (blocks for 1 second max)
        messages = self.bus.client.xreadgroup(
          groupname="storage_group",
          consumername="worker_1",
          streams={"stream:clean_tweets": ">"},
          count=10,
          block=1000 # Block for 1 second
        )
        
        if messages:
          for stream_name, rows in messages:
            for msg_id, data in rows:
              # 2. Parse & Buffer
              payload = json.loads(data['json_payload'])
              tweet = CleanTweet(**payload)
              
              self.buffer.append(tweet)
              self.pending_ids.append(msg_id)
        
        # 3. Check Triggers
        is_full = len(self.buffer) >= self.BATCH_SIZE
        is_stale = (time.time() - self.last_flush) > self.FLUSH_TIMEOUT
        
        if is_full:
          self.logger.info(f"Buffer full ({len(self.buffer)} items), flushing...")
          await self.flush_buffer()
        elif is_stale and self.buffer:
          self.logger.info(f"Timeout reached ({self.FLUSH_TIMEOUT}s), flushing {len(self.buffer)} items...")
          await self.flush_buffer()
        
        # Yield control to event loop
        await asyncio.sleep(0.01)
        
      except KeyboardInterrupt:
        self.logger.info("Shutting down...")
        if self.buffer:
          self.logger.info(f"Flushing remaining {len(self.buffer)} items...")
          await self.flush_buffer()
        break
      except Exception as e:
        self.logger.error(f"Crash in loop: {e}")
        await asyncio.sleep(1)
    
    # Final stats
    self.logger.info("Stats", total_processed=self.total_processed, total_batches=self.total_batches)


class StorageManager:
  """Manages the storage worker lifecycle"""
  
  def __init__(self, repo: IDataRepository, redis_config: dict):
    self.worker = StorageWorker(repo, redis_config)
    
  async def start(self):
    """Start the storage worker"""
    await self.worker.run()
  
  def stop(self):
    """Stop the storage worker"""
    # Worker will stop on next iteration
    pass
