#!/usr/bin/env python3
"""
Run Analytics Worker - Displays live BUY/SELL signals
Run this in a separate terminal to monitor signals
"""
import sys
import os
import time
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shared.bus import RedisBus


def main():
    print("\n" + "="*60)
    print("LIVE SIGNAL MONITOR")
    print("="*60)
    
    print("\n1. Connecting to Redis...")
    bus = RedisBus()
    if not bus.ping():
        print("  ERROR: Redis not running!")
        print("  Start Redis with: redis-server")
        sys.exit(1)
    
    print("  Redis connected")
    
    print("\n2. Subscribing to live signals...")
    print("  Waiting for BUY/SELL signals...")
    print("  Press Ctrl+C to stop")
    print("="*60 + "\n")
    
    pubsub = bus.client.pubsub()
    pubsub.subscribe("channel:live_signals")
    
    try:
        for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    signal = json.loads(message['data'])
                    
                    if signal['signal'] == 'BUY':
                        print("="*60)
                        print(f"BUY SIGNAL - {signal['ticker']}")
                        print("="*60)
                        print(f"Score:      {signal['composite_score']:.3f}")
                        print(f"Confidence: {signal['confidence_score']:.3f}")
                        print(f"Factors:    {', '.join(signal['factors'])}")
                        print(f"Tweet:      {signal['tweet_content'][:60]}...")
                        print("="*60 + "\n")
                    
                    elif signal['signal'] == 'SELL':
                        print("="*60)
                        print(f"SELL SIGNAL - {signal['ticker']}")
                        print("="*60)
                        print(f"Score:      {signal['composite_score']:.3f}")
                        print(f"Confidence: {signal['confidence_score']:.3f}")
                        print(f"Factors:    {', '.join(signal['factors'])}")
                        print(f"Tweet:      {signal['tweet_content'][:60]}...")
                        print("="*60 + "\n")
                    
                    else:
                    # Add this to see the heartbeat
                        print(f"\r[Alive] {signal['ticker']}: {signal['signal']} (Score: {signal['composite_score']:.2f})   ")
                    
                except Exception as e:
                    print(f"Error parsing signal: {e}")
    
    except KeyboardInterrupt:
        print("\n[Main] Stopped")
    finally:
        pubsub.close()


if __name__ == "__main__":
    main()

