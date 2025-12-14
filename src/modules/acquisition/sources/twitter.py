import time
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List
from playwright.sync_api import sync_playwright
from src.shared.interfaces import IDataSource, RawTweet
from src.modules.acquisition.sources.twitter_auth import TwitterAuth
from src.modules.acquisition.sources.twitter_settings import TwitterSettings

class TwitterPlaywrightSource(IDataSource):
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.auth = TwitterAuth()

    def _connect_sync(self):
        """Sync version of connect to run in thread"""
        print("   [TwitterSource] Launching Browser...")
        self.playwright = sync_playwright().start()
        
        # Launch with visible browser for debugging
        chrome_path = "pw-browsers/chromium-1200/chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
        self.browser = self.playwright.chromium.launch(
            headless=False,
            executable_path=chrome_path,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        )
        
        # Create context with realistic settings
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
            }
        )
        
        # Load and inject cookies
        if not self.auth.authenticate(self.context):
            raise Exception("Failed to authenticate. Please provide cookies in config/cookies.json")
        
        # Create page and hide webdriver
        self.page = self.context.new_page()
        
        # Hide automation detection
        self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            window.navigator.chrome = {
                runtime: {}
            };
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
        
        print("   [TwitterSource] ✓ Stealthy browser ready!")

    async def connect(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self._connect_sync)

    def _fetch_sync(self, query: str, limit: int) -> List[RawTweet]:
        """Sync version of fetch to run in thread"""
        # Use hashtag URL format for queries starting with #
        if query.startswith('#'):
            url = f"https://x.com/hashtag/{query[1:]}?f=live"
        else:
            url = f"https://x.com/search?q={query}&f=live"
        
        print(f"   [TwitterSource] Navigating to: {url}")
        
        # Random timeout between min and max (in milliseconds)
        page_timeout = random.randint(
            TwitterSettings.PAGE_LOAD_TIMEOUT_MIN * 1000,
            TwitterSettings.PAGE_LOAD_TIMEOUT_MAX * 1000
        )
        print(f"   [TwitterSource] Page load timeout: {page_timeout/1000:.1f}s")
        
        try:
            self.page.goto(url, timeout=page_timeout)
            
            # No initial wait - page.goto already waited for page load
            print(f"   [TwitterSource] ✓ Page loaded, starting scroll...")
            
            # Multiple scrolls with random waits
            for i in range(TwitterSettings.SCROLL_ITERATIONS):
                scroll_distance = random.randint(
                    TwitterSettings.SCROLL_DISTANCE_MIN,
                    TwitterSettings.SCROLL_DISTANCE_MAX
                )
                scroll_wait = random.uniform(
                    TwitterSettings.SCROLL_WAIT_MIN,
                    TwitterSettings.SCROLL_WAIT_MAX
                )
                
                print(f"   [TwitterSource] 📜 Scroll #{i+1}: {scroll_distance}px, wait {scroll_wait:.1f}s...")
                self.page.evaluate(f"window.scrollBy(0, {scroll_distance})")
                time.sleep(scroll_wait)
            
            # After all scrolling, check what we got
            tweet_count = self.page.locator('article[data-testid="tweet"]').count()
            print(f"   [TwitterSource] ✓ Found {tweet_count} tweets in DOM after scrolling!")
            
            if tweet_count == 0:
                print("   [TwitterSource] ❌ No tweets found after scrolling")
                self.page.screenshot(path="debug_error.png")
                return []
            
        except Exception as e:
            print(f"   [TwitterSource] ❌ Error: {e}")
            self.page.screenshot(path="debug_error.png")
            print("   [TwitterSource] 📸 Saved debug_error.png")
            return []

        # Extract ALL tweets found (no limit during extraction)
        tweets = []
        articles = self.page.locator('article[data-testid="tweet"]').all()
        print(f"   [TwitterSource] 📝 Extracting from {len(articles)} articles...")
        
        for idx, article in enumerate(articles):
            try:
                # Random pause to simulate human reading
                if idx > 0:
                    pause = random.uniform(
                        TwitterSettings.EXTRACTION_PAUSE_MIN,
                        TwitterSettings.EXTRACTION_PAUSE_MAX
                    )
                    time.sleep(pause)
                
                text = article.locator('div[data-testid="tweetText"]').inner_text()
                
                tweets.append(RawTweet(
                    content=text.replace('\n', ' '),
                    source_id="unknown",
                    timestamp=datetime.now().timestamp(),
                    author="unknown",
                    url=url
                ))
                
                print(f"   [TwitterSource] Extracted tweet {idx+1}/{len(articles)}")
                
            except Exception as e:
                print(f"   [TwitterSource] ⚠️  Failed to extract tweet {idx+1}: {e}")
                continue
        
        print(f"   [TwitterSource] ✓ Successfully extracted {len(tweets)} tweets!")
        return tweets[:limit]  # Return only up to limit

    async def fetch_latest(self, query: str, limit: int = 10) -> List[RawTweet]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._fetch_sync, query, limit)

    def _close_sync(self):
        """Sync version of close to run in thread"""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    async def close(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self._close_sync)
        self.executor.shutdown(wait=True)
