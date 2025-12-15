"""
Acquisition Worker - Orchestrates the scraping cycle
Implements the "Super-Query" strategy with long cooldown periods
"""
import time
import random
import asyncio
from multiprocessing import Queue
from typing import List

from src.shared.logger import get_logger
from src.modules.acquisition.sources.twitter import TwitterPlaywrightSource
from src.shared.log_utils import log_print


class AcquisitionWorker:
    """
    Scraper worker that implements the Super-Query strategy:
    1. Scrape multiple hashtags in one big query
    2. Extract 50-100 tweets at once
    3. Sleep 8-12 minutes between cycles
    """
    
    def __init__(self, output_queue: Queue, target_tags: List[str], config: dict):
        self.output_queue = output_queue
        self.tags = target_tags
        self.config = config
        self.source = None
        
        self.cycle_count = 0
        self.total_scraped = 0
    
    def _build_super_query(self) -> str:
        tags_clean = [tag.lstrip('#').strip() for tag in self.tags]
        hashtags = ['#' + tag for tag in tags_clean]
        return " OR ".join(hashtags)
    
    async def run_async(self):
        """Main scraping loop"""
        log_print("[Scraper] Worker started")
        
        # Initialize source in subprocess (after fork)
        self.source = TwitterPlaywrightSource()
        
        try:
            await self.source.connect()
            log_print("[Scraper] Connected to Twitter")
        except Exception as e:
            log_print(f"[Scraper] Failed to connect: {e}")
            return
        
        # Build super query once
        super_query = self._build_super_query()
        query_limit = self.config.get('twitter', {}).get('query_limit', 100)
        cooldown_min = self.config.get('twitter', {}).get('cooldown_min', 480)
        cooldown_max = self.config.get('twitter', {}).get('cooldown_max', 720)
        
        log_print(f"[Scraper] Starting super-cycle")
        log_print(f"[Scraper] Query: {super_query}")
        log_print(f"[Scraper] Limit: {query_limit} tweets")
        log_print(f"[Scraper] Cooldown: {cooldown_min/60:.0f}-{cooldown_max/60:.0f} minutes")
        
        while True:
            try:
                self.cycle_count += 1
                log_print(f"\n[Scraper] Cycle {self.cycle_count}: Query={super_query}")
                
                # The heavy lift - long waits
                tweets = await self.source.fetch_latest(super_query, limit=query_limit)
                
                if tweets:
                    log_print(f"[Scraper] Harvested {len(tweets)} tweets")
                    
                    # Push to processing queue
                    for tweet in tweets:
                        self.output_queue.put(tweet)
                    
                    self.total_scraped += len(tweets)
                else:
                    log_print("[Scraper] Zero tweets found (check selectors/cookies)")
                
                # Cooldown: 8-12 minutes
                sleep_time = random.uniform(cooldown_min, cooldown_max)
                minutes = sleep_time / 60
                
                log_print(f"[Scraper] Cooling down for {minutes:.1f} minutes...")
                log_print(f"[Scraper] Total scraped: {self.total_scraped} tweets")
                
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                log_print("[Scraper] Shutdown requested")
                break
            except Exception as e:
                log_print(f"[Scraper] Cycle {self.cycle_count} failed: {e}")
                time.sleep(60)
        
        await self.source.close()
        log_print(f"[Scraper] Worker stopped. Total: {self.total_scraped} tweets, {self.cycle_count} cycles")
    
    def run_process(self):
        """Entry point for multiprocessing.Process"""
        asyncio.run(self.run_async())
