"""
Log Collector - Subscribes to Redis logs and writes to hourly files
Runs as a separate process to collect logs from all workers
"""
import json
import os
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional
import redis


class LogCollector:
  """
  Collects logs from Redis pub/sub and writes to hourly files
  File format: data/logs/YYYY-MM-DD/HH.log
  """
  
  def __init__(self, log_dir: str = "data/logs", redis_config: Optional[dict] = None):
    self.log_dir = Path(log_dir)
    self.log_dir.mkdir(parents=True, exist_ok=True)
    
    redis_config = redis_config or {'host': 'localhost', 'port': 6379}
    self.redis = redis.Redis(**redis_config, decode_responses=True)
    self.pubsub = self.redis.pubsub()
    
    self.current_file = None
    self.current_hour = None
    self.total_logs = 0
  
  def _get_log_file_path(self) -> Path:
    """Get current log file path (hourly rotation)"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    
    # Create date directory
    date_dir = self.log_dir / date_str
    date_dir.mkdir(exist_ok=True)
    
    return date_dir / f"{hour_str}.log"
  
  def _rotate_if_needed(self):
    """Check if we need to rotate to a new file"""
    current_hour = datetime.now().strftime("%Y-%m-%d-%H")
    
    if self.current_hour != current_hour:
      # Close old file if open
      if self.current_file:
        self.current_file.close()
      
      # Open new file
      log_path = self._get_log_file_path()
      self.current_file = open(log_path, 'a', encoding='utf-8')
      self.current_hour = current_hour
      
      print(f"[LogCollector] Rotated to new log file: {log_path}")
  
  def _write_log(self, log_entry: dict):
    """Write a log entry to the current file"""
    self._rotate_if_needed()
    
    if self.current_file:
      # Write as JSON (one line per log)
      self.current_file.write(json.dumps(log_entry) + '\n')
      self.current_file.flush() # Ensure immediate write
      self.total_logs += 1
  
  async def run(self):
    """
    Main loop: Subscribe to Redis and write logs
    This should run in a separate process/thread
    """
    print("Starting...")
    print(f"[LogCollector] Writing logs to: {self.log_dir}")
    
    # Subscribe to log channel
    self.pubsub.subscribe("channel:logs")
    print("[LogCollector] Subscribed to channel:logs")
    
    try:
      # Listen for messages
      for message in self.pubsub.listen():
        if message['type'] == 'message':
          try:
            # Parse log entry
            log_entry = json.loads(message['data'])
            
            # Write to file
            self._write_log(log_entry)
            
            # Print stats every 100 logs
            if self.total_logs % 100 == 0:
              print(f"[LogCollector] Collected {self.total_logs} logs")
            
            # Yield control to event loop
            await asyncio.sleep(0.001)
            
          except json.JSONDecodeError:
            print(f"[LogCollector] Invalid log format: {message['data']}")
          except Exception as e:
            print(f"[LogCollector] Error processing log: {e}")
    
    except KeyboardInterrupt:
      print("\n[LogCollector] Shutting down...")
    
    finally:
      # Cleanup
      if self.current_file:
        self.current_file.close()
      self.pubsub.close()
      
      print(f"[LogCollector] Final stats: {self.total_logs} logs collected")
  
  def get_stats(self) -> dict:
    """Get collector statistics"""
    return {
      'total_logs': self.total_logs,
      'current_file': str(self._get_log_file_path()),
      'log_dir': str(self.log_dir)
    }


async def main():
  """Standalone runner for log collector"""
  collector = LogCollector()
  await collector.run()


if __name__ == "__main__":
  asyncio.run(main())

