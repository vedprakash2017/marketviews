
# Market System

Real-time Twitter scraper and sentiment analyser for Indian stock market signals.

## Setup

### 1\. Install dependencies

```bash
python3.13 -m venv env
source env/bin/activate

pip install -r requirements.txt
playwright install chromium
```

### 2\. Start Redis

```bash
redis-server
```

### 3\. Add cookies

Export cookies from your browser for `x.com` using the Cookie-Editor extension and save the file to:
`config/cookies.json`

-----

## Usage

**Start the system:**

```bash
python main.py
```

-----

## Monitoring

**Watch live trading signals (consumer for signals):**

```bash
python tests/run_analytics.py
```

**Watch system logs (consumer for logs):**

```bash
python tests/run_log_collector.py
```

-----

## Configuration

Edit `config/settings.yaml` to change targets or timing.

```yaml
acquisition:
  targets: ["#nifty50", "#banknifty", "#sensex"]
  twitter:
    cooldown_min: 480  # 8 minutes
    query_limit: 100   # tweets per cycle
```

-----

## Architecture

1.  **Scraper**: Collects tweets using Playwright (stealth mode).
2.  **Processor**: Cleans text and removes duplicates (Redis).
3.  **Analytics**: Scores sentiment + user credibility + viral stats.
4.  **Storage**: Saves raw data to Parquet files.

-----