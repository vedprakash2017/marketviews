import asyncio
from src.shared.interfaces import IDataSource

class AcquisitionWorker:
    def __init__(self, source: IDataSource):
        self.source = source # <--- INJECTION HAPPENS HERE

    async def run_test_cycle(self, query: str):
        print("[Worker] Starting Test Cycle...")
        await self.source.connect()
        
        try:
            print(f"[Worker] Fetching tweets for {query}...")
            tweets = await self.source.fetch_latest(query, limit=5)
            
            print("\n" + "="*50)
            print(f"SUCCESS! Scraped {len(tweets)} Tweets:")
            for i, t in enumerate(tweets):
                print(f"{i+1}. {t.content[:100]}...")
            print("="*50 + "\n")
            
        finally:
            print("[Worker] Closing resources...")
            await self.source.close()
