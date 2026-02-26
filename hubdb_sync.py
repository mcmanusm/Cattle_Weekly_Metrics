"""
HubDB Sync Script
Syncs data from SQL Server data warehouse to HubSpot HubDB table
"""

import pyodbc
import requests
import json
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
HUBSPOT_TOKEN = os.getenv('HUBSPOT_TOKEN')
HUBDB_TABLE_ID = os.getenv('HUBDB_TABLE_ID')
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# HubSpot API endpoint
HUBDB_API_URL = f"https://api.hubapi.com/cms/v3/hubdb/tables/{HUBDB_TABLE_ID}"


def log(msg: str):
    """Timestamped print"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_database_connection():
    """Create connection to SQL Server"""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USERNAME};"
        f"PWD={DB_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def fetch_data_from_warehouse() -> List[Dict]:
    """Fetch data from the data warehouse"""
    query = """
    SELECT * FROM [ReportingView].[Cattle_Weekly_Metrics]
    ORDER BY week_index ASC
    """

    log("Connecting to data warehouse...")
    conn = get_database_connection()
    cursor = conn.cursor()

    log("Executing query...")
    cursor.execute(query)

    # Get column names
    columns = [column[0] for column in cursor.description]
    log(f"Columns found: {', '.join(columns)}")

    # Fetch all rows
    log("Fetching rows...")
    rows = cursor.fetchall()
    log(f"Fetched {len(rows)} rows — now converting types...")

    # Convert to list of dicts, serialising date/datetime to strings
    data = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            if isinstance(value, datetime):
                # HubDB date columns expect Unix timestamp in milliseconds (not a string)
                row_dict[columns[i]] = int(value.timestamp() * 1000)
            elif isinstance(value, date):
                # Convert date to midnight UTC timestamp in milliseconds
                row_dict[columns[i]] = int(datetime(value.year, value.month, value.day).timestamp() * 1000)
            elif isinstance(value, Decimal):
                # SQL DECIMAL/NUMERIC columns come through as Python Decimal — convert to float
                row_dict[columns[i]] = float(value)
            elif value is None:
                row_dict[columns[i]] = None
            else:
                row_dict[columns[i]] = value
        data.append(row_dict)

    conn.close()
    log(f"✓ Data ready — {len(data)} rows")
    return data


def clear_hubdb_table():
    """Clear all existing rows from HubDB table using batch delete (much faster)"""
    headers = {
        'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        'Content-Type': 'application/json'
    }

    log("Fetching existing rows to delete...")

    # HubDB paginates at 1000 rows max — collect all pages
    all_row_ids = []
    after = None

    while True:
        params = {'limit': 1000}
        if after:
            params['after'] = after

        response = requests.get(
            f"{HUBDB_API_URL}/rows/draft",
            headers=headers,
            params=params,
            verify=False
        )

        if response.status_code != 200:
            log(f"✗ Error fetching rows: {response.status_code} — {response.text}")
            return

        body = response.json()
        results = body.get('results', [])
        all_row_ids.extend([r['id'] for r in results])

        # Check for next page
        paging = body.get('paging', {})
        next_cursor = paging.get('next', {}).get('after')
        if next_cursor:
            after = next_cursor
        else:
            break

    log(f"Found {len(all_row_ids)} existing rows")

    if not all_row_ids:
        log("Nothing to delete — table already empty")
        return

    # Batch delete in groups of 100 (HubSpot limit per batch call)
    batch_size = 100
    total_deleted = 0
    total_batches = (len(all_row_ids) + batch_size - 1) // batch_size

    for i in range(0, len(all_row_ids), batch_size):
        batch_ids = all_row_ids[i:i + batch_size]
        payload = {"inputs": batch_ids}

        delete_response = requests.post(
            f"{HUBDB_API_URL}/rows/draft/batch/purge",
            headers=headers,
            json=payload,
            verify=False
        )

        batch_num = i // batch_size + 1
        if delete_response.status_code in [200, 204]:
            total_deleted += len(batch_ids)
            log(f"  Deleted batch {batch_num}/{total_batches} ({total_deleted}/{len(all_row_ids)} rows)")
        else:
            log(f"  ✗ Delete batch {batch_num} failed: {delete_response.status_code} — {delete_response.text}")

    log(f"✓ Cleared {total_deleted} rows")


def transform_data_for_hubdb(data: List[Dict]) -> List[Dict]:
    """Transform warehouse rows into HubDB row format"""
    return [{"values": row} for row in data]


def batch_insert_rows(rows: List[Dict], batch_size: int = 100):
    """Insert rows into HubDB in batches"""
    headers = {
        'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        'Content-Type': 'application/json'
    }

    total_rows = len(rows)
    total_batches = (total_rows + batch_size - 1) // batch_size
    successful = 0
    failed = 0

    log(f"Inserting {total_rows} rows across {total_batches} batches of {batch_size}...")

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        batch_num = i // batch_size + 1

        response = requests.post(
            f"{HUBDB_API_URL}/rows/draft/batch/create",
            headers=headers,
            json={"inputs": batch},
            verify=False
        )

        if response.status_code in [200, 201]:
            successful += len(batch)
            log(f"  ✓ Batch {batch_num}/{total_batches} inserted ({successful}/{total_rows} rows done)")
        else:
            failed += len(batch)
            log(f"  ✗ Batch {batch_num}/{total_batches} failed: {response.status_code}")
            log(f"    Detail: {response.text[:300]}")  # cap output so it doesn't flood the console

    log(f"Insert complete — {successful} succeeded, {failed} failed")
    return successful, failed


def publish_table():
    """Publish the HubDB table to make changes live"""
    headers = {
        'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        'Content-Type': 'application/json'
    }

    log("Publishing table to make changes live...")
    response = requests.post(
        f"{HUBDB_API_URL}/draft/publish",
        headers=headers,
        verify=False  # added verify=False to match rest of script
    )

    if response.status_code in [200, 201]:
        log("✓ Table published successfully!")
        return True
    else:
        log(f"✗ Publish failed: {response.status_code} — {response.text}")
        return False


def main():
    """Main sync function"""
    start = time.time()

    print()
    print("=" * 60)
    print("  HubDB Sync — Starting")
    print("=" * 60)
    print()

    try:
        # Step 1: Fetch from warehouse
        warehouse_data = fetch_data_from_warehouse()

        if not warehouse_data:
            log("No data returned from warehouse — exiting")
            return

        print()

        # Step 2: Clear existing HubDB rows
        clear_hubdb_table()

        print()

        # Step 3: Transform
        log("Transforming rows to HubDB format...")
        hubdb_data = transform_data_for_hubdb(warehouse_data)
        log(f"✓ {len(hubdb_data)} rows ready to insert")

        print()

        # Step 4: Insert
        successful, failed = batch_insert_rows(hubdb_data)

        print()

        # Step 5: Publish
        if successful > 0:
            publish_table()

        elapsed = time.time() - start
        print()
        print("=" * 60)
        print(f"  Sync Complete — {successful} rows in {elapsed:.1f}s")
        if failed:
            print(f"  ⚠ {failed} rows failed — check logs above")
        print("=" * 60)
        print()

    except Exception as e:
        log(f"FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()