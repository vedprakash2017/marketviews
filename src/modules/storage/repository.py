"""
Parquet Repository - Handles physical disk operations
Organizes data by date/hour partitions for efficient querying
"""
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from src.shared.interfaces import IDataRepository
from src.shared.types import CleanTweet
from src.shared.logger import get_logger


class ParquetRepository(IDataRepository):
  """
  Saves tweets to Parquet files with:
  - Date/Hour partitioning
  - Snappy compression
  - PyArrow for performance
  """
  
  def __init__(self, base_path: str = "data/raw", compression: str = "snappy", redis_config: Optional[dict] = None):
    self.base_path = Path(base_path)
    self.compression = compression
    self.base_path.mkdir(parents=True, exist_ok=True)
    self.logger = get_logger("ParquetRepository", redis_config=redis_config or {'host': 'localhost', 'port': 6379})
    
  def _get_partition_folder(self, timestamp: float) -> Path:
    """
    Organizes data by Date and Hour.
    Example: data/raw/2025-12-15/14/
    
    Args:
      timestamp: Unix timestamp
      
    Returns:
      Path to partition folder
    """
    dt = datetime.fromtimestamp(timestamp)
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")
    
    folder = self.base_path / date_str / hour_str
    folder.mkdir(parents=True, exist_ok=True)
    return folder
  
  def save_batch(self, tweets: List[CleanTweet]):
    """
    Save a batch of tweets to Parquet with compression.
    
    Args:
      tweets: List of CleanTweet objects
      
    Raises:
      Exception: If write fails (prevents ACK in worker)
    """
    if not tweets:
      return
    
    try:
      # 1. Convert to DataFrame
      # model_dump() is the Pydantic v2 method for to_dict()
      data_dicts = [t.model_dump() for t in tweets]
      df = pd.DataFrame(data_dicts)
      
      # 2. Determine Partition
      # We use the timestamp of the first tweet to decide the folder
      # In a strict system, we might split the batch if it spans hours,
      # but for this assignment, this approximation is acceptable.
      folder = self._get_partition_folder(tweets[0].timestamp)
      
      # 3. Generate Unique Filename
      # batch_{timestamp_ms}.parquet
      filename = f"batch_{int(datetime.now().timestamp() * 1000)}.parquet"
      file_path = folder / filename
      
      # 4. Write with PyArrow (Snappy Compression)
      table = pa.Table.from_pandas(df)
      pq.write_table(
        table, 
        file_path, 
        compression=self.compression
      )
      
      self.logger.info(f"Saved {len(tweets)} tweets to {file_path}")
      
    except Exception as e:
      self.logger.error(f"Failed to write batch: {e}")
      raise e # Re-raise to prevent ACK in worker
  
  def load_range(self, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Load tweets within a time range.
    
    Args:
      start: Start datetime
      end: End datetime
      
    Returns:
      DataFrame with all tweets in range
    """
    all_dataframes = []
    
    # Iterate through all partition folders
    for date_folder in self.base_path.iterdir():
      if not date_folder.is_dir():
        continue
        
      # Check if date is in range
      try:
        folder_date = datetime.strptime(date_folder.name, "%Y-%m-%d")
        if not (start.date() <= folder_date.date() <= end.date()):
          continue
      except ValueError:
        continue
      
      # Iterate through hour folders
      for hour_folder in date_folder.iterdir():
        if not hour_folder.is_dir():
          continue
        
        # Read all parquet files in this hour
        for parquet_file in hour_folder.glob("*.parquet"):
          df = pd.read_parquet(parquet_file)
          
          # Filter by exact timestamp
          df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='s')
          df = df[(df['timestamp_dt'] >= start) & (df['timestamp_dt'] <= end)]
          
          if not df.empty:
            all_dataframes.append(df)
    
    if all_dataframes:
      result = pd.concat(all_dataframes, ignore_index=True)
      result = result.sort_values('timestamp')
      return result
    else:
      return pd.DataFrame()
  
  def get_stats(self) -> dict:
    """Get storage statistics"""
    total_files = 0
    total_size = 0
    
    for parquet_file in self.base_path.rglob("*.parquet"):
      total_files += 1
      total_size += parquet_file.stat().st_size
    
    return {
      'total_files': total_files,
      'total_size_mb': total_size / (1024 * 1024),
      'base_path': str(self.base_path)
    }

