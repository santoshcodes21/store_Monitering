# Store Monitoring Backend by [Your First Name] for Loop Interview, August 2025
# Custom implementation for tracking restaurant uptime/downtime

import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, time
from typing import List, Tuple, Dict, Optional
import pandas as pd
import pytz
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import urllib.request
import zipfile
import io

# Initialize FastAPI application for [Your First Name]'s store monitoring
app = FastAPI(title="LoopStoreMonitorAPI")

# Database configuration
DB_PATH = 'loop_stores.db'  # Unique DB name
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# Initialize database tables for store monitoring
def setup_loop_database():
    """[Your First Name]: Sets up SQLite tables with indices for performance."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS loop_timezones (
            store_id INTEGER PRIMARY KEY,
            timezone_str TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS loop_business_hours (
            store_id INTEGER,
            day_of_week INTEGER,
            open_time_local TEXT,
            close_time_local TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS loop_store_status (
            store_id INTEGER,
            timestamp_utc REAL,
            status TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_loop_status ON loop_store_status (store_id, timestamp_utc)')
    conn.commit()

# Download and extract data files
def fetch_and_unzip_data():
    """[Your First Name]: Downloads and extracts store data if CSVs are missing."""
    csv_files = ['store_status.csv', 'business_hours.csv', 'timezones.csv']
    if all(os.path.exists(f) for f in csv_files):
        return
    data_url = 'https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip'
    try:
        with urllib.request.urlopen(data_url) as response:
            zip_content = response.read()
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            z.extractall('.')
        # Handle potential file name variations
        if os.path.exists('store status.csv'):
            os.rename('store status.csv', 'store_status.csv')
        if os.path.exists('Menu.csv'):
            os.rename('Menu.csv', 'business_hours.csv')
    except Exception as e:
        raise RuntimeError(f"[Your First Name]: Failed to fetch/unzip data: {e}")

# Load CSV data into database
def load_loop_data():
    """[Your First Name]: Loads CSV data into SQLite tables."""
    fetch_and_unzip_data()
    try:
        cursor.execute('DELETE FROM loop_timezones')
        cursor.execute('DELETE FROM loop_business_hours')
        cursor.execute('DELETE FROM loop_store_status')

        tz_df = pd.read_csv('timezones.csv')
        for _, row in tz_df.iterrows():
            cursor.execute('INSERT OR REPLACE INTO loop_timezones (store_id, timezone_str) VALUES (?, ?)',
                          (int(row['store_id']), row['timezone_str']))

        hours_df = pd.read_csv('business_hours.csv')
        for _, row in hours_df.iterrows():
            cursor.execute('INSERT INTO loop_business_hours (store_id, day_of_week, open_time_local, close_time_local) VALUES (?, ?, ?, ?)',
                          (int(row['store_id']), int(row['dayOfWeek']), row['start_time_local'], row['end_time_local']))

        status_df = pd.read_csv('store_status.csv')
        for _, row in status_df.iterrows():
            ts = pd.to_datetime(row['timestamp_utc']).timestamp()
            cursor.execute('INSERT INTO loop_store_status (store_id, timestamp_utc, status) VALUES (?, ?, ?)',
                          (int(row['store_id']), ts, row['status']))

        conn.commit()
    except Exception as e:
        raise RuntimeError(f"[Your First Name]: Error loading data: {e}")

# Track report status
report_tracker: Dict[str, Dict[str, Optional[str]]] = {}

# Compute business intervals
def calculate_business_periods(
    store_id: int,
    period_start: datetime,
    period_end: datetime,
    tz: pytz.BaseTzInfo
) -> List[Tuple[float, float]]:
    """[Your First Name]: Converts local business hours to UTC intervals."""
    hours = cursor.execute(
        'SELECT day_of_week, open_time_local, close_time_local FROM loop_business_hours WHERE store_id=?',
        (store_id,)
    ).fetchall()
    if not hours:
        return [(period_start.timestamp(), period_end.timestamp())]

    periods: List[Tuple[float, float]] = []
    local_start = period_start.astimezone(tz)
    local_end = period_end.astimezone(tz)
    current_day = local_start.date()
    while current_day <= local_end.date():
        dow = current_day.weekday()
        day_hours = [h for h in hours if h[0] == dow]
        for _, start_str, end_str in day_hours:
            try:
                start_t = time.fromisoformat(start_str)
                end_t = time.fromisoformat(end_str)
                start_local_dt = datetime.combine(current_day, start_t)
                end_local_dt = datetime.combine(current_day, end_t)
                start_utc = tz.localize(start_local_dt).astimezone(pytz.utc)
                end_utc = tz.localize(end_local_dt).astimezone(pytz.utc)
                inter_start_ts = max(period_start.timestamp(), start_utc.timestamp())
                inter_end_ts = min(period_end.timestamp(), end_utc.timestamp())
                if inter_start_ts < inter_end_ts:
                    periods.append((inter_start_ts, inter_end_ts))
            except ValueError:
                continue
        current_day += timedelta(days=1)
    return sorted(periods)

# Compute uptime/downtime
def evaluate_store_metrics(
    store_id: int,
    period_start: datetime,
    period_end: datetime,
    tz: pytz.BaseTzInfo,
    unit: str
) -> Tuple[float, float]:
    """[Your First Name]: Calculates uptime/downtime for a store and period."""
    business_periods = calculate_business_periods(store_id, period_start, period_end, tz)
    if not business_periods:
        return 0.0, 0.0

    min_ts = period_start.timestamp() - 86400
    polls = cursor.execute(
        'SELECT timestamp_utc, status FROM loop_store_status WHERE store_id=? AND timestamp_utc >= ? ORDER BY timestamp_utc',
        (store_id, min_ts)
    ).fetchall()
    if not polls:
        return 0.0, sum(e - s for s, e in business_periods) / (60 if unit == 'min' else 3600)

    ts_list: List[float] = [row[0] for row in polls]
    statuses: List[str] = [row[1] for row in polls]
    n = len(ts_list)

    uptime_seconds = 0.0
    for b_start_ts, b_end_ts in business_periods:
        current_t_ts = b_start_ts
        i = 0
        while current_t_ts < b_end_ts and i < n:
            segment_start_ts = ts_list[i]
            segment_end_ts = ts_list[i + 1] if i + 1 < n else 1e10
            if i == 0 and current_t_ts < ts_list[0]:
                inter_end_ts = min(b_end_ts, ts_list[0])
                if statuses[0] == 'active':
                    uptime_seconds += inter_end_ts - current_t_ts
                current_t_ts = inter_end_ts
                continue
            inter_start_ts = max(current_t_ts, segment_start_ts)
            inter_end_ts = min(b_end_ts, segment_end_ts)
            if inter_start_ts < inter_end_ts:
                if statuses[i] == 'active':
                    uptime_seconds += inter_end_ts - inter_start_ts
                current_t_ts = inter_end_ts
            i += 1

    total_business_seconds = sum(e - s for s, e in business_periods)
    downtime_seconds = total_business_seconds - uptime_seconds
    factor = 60.0 if unit == 'min' else 3600.0
    return uptime_seconds / factor, downtime_seconds / factor

# Generate report
def compile_store_report(report_id: str):
    """[Your First Name]: Generates uptime/downtime report and saves CSV."""
    try:
        stores = [row[0] for row in cursor.execute('SELECT DISTINCT store_id FROM loop_store_status').fetchall()]
        if not stores:
            report_tracker[report_id]['status'] = 'complete'
            report_tracker[report_id]['file'] = None
            return

        current_ts = cursor.execute('SELECT MAX(timestamp_utc) FROM loop_store_status').fetchone()[0]
        if not current_ts:
            report_tracker[report_id]['status'] = 'complete'
            report_tracker[report_id]['file'] = None
            return
        current_time = datetime.fromtimestamp(current_ts, tz=pytz.utc)

        results = []
        periods = {
            'last_hour': {'delta': timedelta(minutes=60), 'unit': 'min'},
            'last_day': {'delta': timedelta(hours=24), 'unit': 'hour'},
            'last_week': {'delta': timedelta(days=7), 'unit': 'hour'}
        }

        for store in stores:
            tz_row = cursor.execute('SELECT timezone_str FROM loop_timezones WHERE store_id=?', (store,)).fetchone()
            tz_str = tz_row[0] if tz_row and tz_row[0] in pytz.all_timezones else 'America/Chicago'
            tz = pytz.timezone(tz_str)

            store_metrics = {'store_id': store}
            for p_name, p_info in periods.items():
                period_start = current_time - p_info['delta']
                uptime, downtime = evaluate_store_metrics(store, period_start, current_time, tz, p_info['unit'])
                store_metrics[f'uptime_{p_name}'] = round(uptime, 2)
                store_metrics[f'downtime_{p_name}'] = round(downtime, 2)
            results.append(store_metrics)

        df = pd.DataFrame(results)
        os.makedirs('loop_reports', exist_ok=True)
        csv_path = f'loop_reports/report_{report_id}.csv'
        df.to_csv(csv_path, index=False)

        report_tracker[report_id]['status'] = 'complete'
        report_tracker[report_id]['file'] = csv_path
    except Exception as e:
        print(f"[Your First Name]: Error in report {report_id}: {e}")
        report_tracker[report_id]['status'] = 'error'
        report_tracker[report_id]['file'] = None

# Initialize
setup_loop_database()
load_loop_data()

# APIs
@app.post("/trigger_report")
async def initiate_report():
    """[Your First Name]: Triggers a new report generation."""
    report_id = str(uuid.uuid4())
    report_tracker[report_id] = {'status': 'running', 'file': None}
    threading.Thread(target=compile_store_report, args=(report_id,)).start()
    return {"report_id": report_id}

@app.get("/get_report/{report_id}")
async def retrieve_report(report_id: str):
    """[Your First Name]: Retrieves report status or CSV."""
    if report_id not in report_tracker:
        raise HTTPException(status_code=404, detail="Invalid report_id")
    stat = report_tracker[report_id]['status']
    if stat == 'running':
        return {"status": "Running"}
    elif stat == 'complete' and report_tracker[report_id]['file']:
        return FileResponse(
            report_tracker[report_id]['file'],
            media_type='text/csv',
            filename=f'loop_report_{report_id}.csv'
        )
    else:
        raise HTTPException(status_code=500, detail="Report generation failed or no data")