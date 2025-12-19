import json
import os
from typing import Optional
from pathlib import Path
from playwright.sync_api import BrowserContext
from src.shared.logger import get_logger

class TwitterAuth:
    def __init__(self, cookie_path: str = "config/cookies.json", redis_config: Optional[dict] = None):
        self.cookie_path = Path(cookie_path)
        self.logger = get_logger("TwitterAuth", redis_config=redis_config or {'host': 'localhost', 'port': 6379})

    def authenticate(self, context: BrowserContext) -> bool:
        if not self.cookie_path.exists():
            self.logger.critical(f"No cookie file found at {self.cookie_path}")
            self.logger.critical("Please export cookies from your browser and save them.")
            return False

        try:
            with open(self.cookie_path, 'r') as f:
                cookies = json.load(f)

            clean_cookies = []
            for c in cookies:
                new_cookie = {
                    'name': c['name'],
                    'value': c['value'],
                    'domain': c['domain'],
                    'path': c['path'],
                    'secure': c.get('secure', True),
                    'httpOnly': c.get('httpOnly', False),
                    'sameSite': 'Lax',
                    'expires': c.get('expirationDate', int(c.get('expires', -1)))
                }
                clean_cookies.append(new_cookie)

            context.add_cookies(clean_cookies)
            self.logger.info(f"Injected {len(clean_cookies)} cookies")
            return True

        except Exception as e:
            self.logger.error(f"Failed to inject cookies: {e}")
            return False
