# Market Intelligence System

Real-time Twitter scraper and sentiment analyzer for Indian stock market signals.

## Setup

# 1. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 2. Start Redis
redis-server

# 3. Add cookies
# Export cookies from your browser for x.com and save as:
# config/cookies.json## Usage

# Start the system
python main.py## Monitoring

# Watch live trading signals, consumer for signals
python tests/run_analytics.py

# Watch system logs, consumer for logs
python tests/run_log_collector.py## Configuration

Edit `config/settings.yaml` to change targets or timing:

acquisition:
  targets: ["#nifty50", "#banknifty", "#sensex"]
  twitter:
    cooldown_min: 480  # 8 minutes
    query_limit: 100   # tweets per cycle## Architecture

1. **Scraper**: Collects tweets using Playwright (stealth mode)
2. **Processor**: Cleans text and removes duplicates (Redis)
3. **Analytics**: Scores sentiment + user credibility + viral stats
4. **Storage**: Saves raw data to Parquet files