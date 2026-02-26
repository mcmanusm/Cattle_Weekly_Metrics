# HubDB Sync Script

This script syncs data from your SQL Server data warehouse to HubSpot HubDB.

## Setup Instructions

### 1. Install Dependencies

Open your terminal and run:

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Edit the `.env` file and add your database credentials:

```
DB_USERNAME=your_actual_username
DB_PASSWORD=your_actual_password
```

The HubSpot token and table ID are already configured.

### 3. Run the Script

```bash
python hubdb_sync.py
```

## What the Script Does

1. **Fetches data** from `[ReportingView].[Cattle_Weekly_Metrics]` in your data warehouse
2. **Clears** existing rows in the HubDB table
3. **Inserts** all new data in batches
4. **Publishes** the table to make changes live

## Automation Options

### Option 1: Manual (What you have now)
- Run `python hubdb_sync.py` whenever you want to update

### Option 2: Windows Task Scheduler
- Schedule this script to run daily/weekly automatically

### Option 3: GitHub Actions (Recommended)
- Push this code to GitHub
- Set up GitHub Actions to run on a schedule
- We can set this up next!

## Troubleshooting

### "ODBC Driver 18 for SQL Server not found"
Download and install: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### "Connection failed"
Check your database credentials in the `.env` file

### "401 Unauthorized" from HubSpot
Check your HubSpot token is correct
