import json
import os
from src.shared.log_utils import log_print
from pathlib import Path
from playwright.sync_api import BrowserContext

class TwitterAuth:
    def __init__(self, cookie_path: str = "config/cookies.json"):
        self.cookie_path = Path(cookie_path)

    def authenticate(self, context: BrowserContext) -> bool:
        if not self.cookie_path.exists():
            log_print(f"   [Auth] CRITICAL: No cookie file found at {self.cookie_path}")
            log_print("   [Auth] Please export cookies from your browser and save them.")
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
            log_print(f"   [Auth] Injected {len(clean_cookies)} cookies")
            return True

        except Exception as e:
            log_print(f"   [Auth] Failed to inject cookies: {e}")
            return False
