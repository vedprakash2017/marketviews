#!/usr/bin/env python3
"""
Main Orchestrator - Runs all modules as separate processes
Implements distributed market intelligence system
"""
import time
import os
import sys
import signal
import asyncio
import threading
from multiprocessing import Queue, Process

from src.shared.utils import load_config
from src.shared.log_utils import log_print
from src.shared.log_collector import LogCollector
from src.modules.acquisition.worker import AcquisitionWorker
from src.modules.processing.worker import ProcessingManager
from src.modules.analytics.worker import AnalyticsWorker
from src.modules.storage.worker import StorageWorker
from src.modules.storage.repository import ParquetRepository


# Global processes list
processes = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    log_print("\n" + "="*60)
    log_print("STOPPING SYSTEM...")
    log_print("="*60)
    
    for p in processes:
        if p.is_alive():
            log_print("Stopping...")
            p.terminate()
            p.join(timeout=2)
    
    log_print("  Shutdown complete")
    sys.exit(0)


def run_storage_process(redis_config):
    """Wrapper to run async Storage Worker in a separate process"""
    repo = ParquetRepository()
    worker = StorageWorker(repo, redis_config)
    asyncio.run(worker.run())


def run_analytics_process(redis_config):
    """Wrapper for Analytics Worker"""
    worker = AnalyticsWorker(redis_config)
    worker.run()


import threading
from src.shared.log_collector import LogCollector

def run_log_collector_thread(collector):
    """Run async log collector in a separate thread"""
    import asyncio
    asyncio.run(collector.run())

def main():
    log_print("\n" + "="*60)
    log_print("MARKET INTELLIGENCE SYSTEM - STARTING")
    log_print("="*60)
    
    # Load config
    log_print("\n1. Loading configuration...")
    try:
        config = load_config("config/settings.yaml")
        log_print(f"  Config loaded: {config['app']['name']}")
    except Exception as e:
        log_print(f"  Error loading config: {e}")
        sys.exit(1)
    
    # Create dirs
    os.makedirs("data/logs", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    
    # Start Log Collector (Background Thread)
    # This ensures logs are saved to data/logs/
    log_collector = LogCollector(log_dir="data/logs")
    log_thread = threading.Thread(
        target=run_log_collector_thread, 
        args=(log_collector,), 
        daemon=True
    )
    log_thread.start()
    log_print("  Log Collector started (saving to data/logs/)")
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Shared queue
    raw_data_queue = Queue(maxsize=1000)
    
    redis_config = config.get('redis', {'host': 'localhost', 'port': 6379})
    targets = config['acquisition']['targets']
    
    log_print("Starting...")
    log_print(f"  Targets: {', '.join(targets)}")
    log_print(f"  Redis: {redis_config['host']}:{redis_config['port']}")
    
    try:
        # A. Storage Worker
        p_storage = Process(
            target=run_storage_process,
            args=(redis_config,),
            name="StorageWorker"
        )
        p_storage.start()
        processes.append(p_storage)
        log_print(f"  Started Storage (PID: {p_storage.pid})")
        time.sleep(1)
        
        # B. Analytics Worker
        p_analytics = Process(
            target=run_analytics_process,
            args=(redis_config,),
            name="AnalyticsWorker"
        )
        p_analytics.start()
        processes.append(p_analytics)
        log_print(f"  Started Analytics (PID: {p_analytics.pid})")
        time.sleep(1)
        
        # C. Processing Workers
        processor = ProcessingManager(raw_data_queue)
        processor.start(workers_count=2)
        log_print(f"  Started Processing workers (2)")
        time.sleep(1)
        
        # D. Scraper Worker
        scraper = AcquisitionWorker(
            output_queue=raw_data_queue,
            target_tags=targets,
            config=config['acquisition']
        )
        
        p_scraper = Process(
            target=scraper.run_process,
            name="ScraperWorker"
        )
        p_scraper.start()
        processes.append(p_scraper)
        log_print(f"  Started Scraper (PID: {p_scraper.pid})")
        
        log_print("\n" + "="*60)
        log_print("SYSTEM RUNNING")
        log_print("="*60)
        log_print("\nPress Ctrl+C to stop")
        log_print("="*60 + "\n")
        
        # Health check loop
        while True:
            time.sleep(10)
            
            # Check if any process died
            for p in processes:
                if not p.is_alive():
                    log_print(f"\n[System] WARNING: {p.name} died (PID: {p.pid})")
            
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        log_print(f"\n[System] Fatal error: {e}")
        signal_handler(None, None)


if __name__ == "__main__":
    main()
