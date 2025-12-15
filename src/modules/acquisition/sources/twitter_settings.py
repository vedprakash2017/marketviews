"""
Twitter scraper settings - loads from config/settings.yaml
"""
import yaml
from pathlib import Path

class TwitterSettings:
  def __init__(self):
    config_path = Path("config/settings.yaml")
    if not config_path.exists():
      # Fallback to hardcoded values if no config
      self._use_defaults()
      return
      
    try:
      with open(config_path) as f:
        config = yaml.safe_load(f)
      
      twitter_config = config['acquisition']['twitter']
      
      # Page load timeout (in seconds) - VERY LONG
      self.PAGE_LOAD_TIMEOUT_MIN = twitter_config.get('page_load_timeout_min', 120)
      self.PAGE_LOAD_TIMEOUT_MAX = twitter_config.get('page_load_timeout_max', 200)
      
      # Scroll settings - VERY LONG WAITS
      self.SCROLL_ITERATIONS = twitter_config.get('scroll_iterations', 3)
      self.SCROLL_DISTANCE_MIN = twitter_config.get('scroll_distance_min', 500)
      self.SCROLL_DISTANCE_MAX = twitter_config.get('scroll_distance_max', 1000)
      self.SCROLL_WAIT_MIN = twitter_config.get('scroll_wait_min', 25)
      self.SCROLL_WAIT_MAX = twitter_config.get('scroll_wait_max', 45)
      
      # Extraction settings
      self.EXTRACTION_PAUSE_MIN = twitter_config.get('extraction_pause_min', 1)
      self.EXTRACTION_PAUSE_MAX = twitter_config.get('extraction_pause_max', 3)
    except Exception as e:
      print(f" Warning: Could not load settings.yaml: {e}")
      print("  Using default values...")
      self._use_defaults()
  
  def _use_defaults(self):
    """Fallback to hardcoded defaults"""
    self.PAGE_LOAD_TIMEOUT_MIN = 120
    self.PAGE_LOAD_TIMEOUT_MAX = 200
    self.SCROLL_ITERATIONS = 3
    self.SCROLL_DISTANCE_MIN = 500
    self.SCROLL_DISTANCE_MAX = 1000
    self.SCROLL_WAIT_MIN = 25
    self.SCROLL_WAIT_MAX = 45
    self.EXTRACTION_PAUSE_MIN = 1
    self.EXTRACTION_PAUSE_MAX = 3

# Create singleton instance
TwitterSettings = TwitterSettings()

