# Nenner Signal Engine - Quickstart

## Setup (5 minutes)

### 1. Install Python dependency
```
pip install python-dotenv
```

### 2. Create Gmail App Password
- Go to https://myaccount.google.com/apppasswords
- Select "Mail" and "Windows Computer"
- Copy the 16-character password

### 3. Configure credentials
Copy `.env.template` to `.env` in the same folder as `nenner_engine.py`:
```
copy .env.template .env
```
Edit `.env` and fill in your Gmail address and App Password.

Alternatively, if using Azure Key Vault, set those variables instead.

### 4. Place both files in a working directory
```
C:\Users\YourName\NennerEngine\
    nenner_engine.py
    .env
```

## Usage

### Backfill all 1,900+ historical emails
```
python nenner_engine.py --backfill
```
This connects to Gmail, pulls every email from `newsletter@charlesnenner.com`,
parses all signals, and stores them in `nenner_signals.db` (SQLite).

Expect ~15-20 minutes for 1,900 emails depending on connection speed.

### Check for new emails (daily use)
```
python nenner_engine.py
```
Pulls only emails since the last run. Skips duplicates automatically.

### Import local .eml files
```
python nenner_engine.py --import-folder "C:\path\to\eml\files"
```

### View current signal state
```
python nenner_engine.py --status
```

### View signal history for an instrument
```
python nenner_engine.py --history Gold
python nenner_engine.py --history TSLA
python nenner_engine.py --history "S&P"
```

### Export to CSV (for Excel analysis)
```
python nenner_engine.py --export
```
Creates `nenner_signals.csv`, `nenner_cycles.csv`, `nenner_price_targets.csv`, and `nenner_emails.csv`.

## Automate with Task Scheduler

To check for new emails every 5 minutes during market hours:

1. Open Task Scheduler
2. Create Basic Task > "Nenner Signal Check"
3. Trigger: Daily, repeat every 5 minutes for 8 hours
4. Action: Start Program
   - Program: `python`
   - Arguments: `C:\Users\YourName\NennerEngine\nenner_engine.py`
   - Start in: `C:\Users\YourName\NennerEngine`

## Database

The SQLite database `nenner_signals.db` contains four tables:

- **emails** - Raw email metadata and classification
- **signals** - Buy/sell signals with origin prices, cancel levels, trigger levels
- **cycles** - Cycle direction data (daily, weekly, monthly, hourly)
- **price_targets** - Upside/downside price targets with reached tracking

You can query it directly from Python, any SQLite browser (e.g., DB Browser for SQLite), 
or connect from Excel via ODBC.

## What's Next

This is Layer 1 (Email Parser + Database) from the strategy document. 
Layer 2 (Alert Engine with T1 Excel bridge) builds on top of this database.
