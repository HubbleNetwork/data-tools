# merge_life360_parquet.py
# merge Life360 24 hour parquet files into a single file with H3_7_STR column - h3 preferred format in Carto is hex string.

import os
import re
import pandas as pd
from datetime import datetime, timezone
import h3 

DATA_DIR = "life360-data"
OUTPUT_FILE = "life360-data/world_locations_aggregate_24_hours_h3_str.parquet"
FILENAME_PATTERN = re.compile(r"(\d{4})_(\d{2})_(\d{2})_h(\d{2})_h3_level7\.parquet")

def extract_timestamp_from_filename(filename):
    """
    Extracts the date and hour from the filename and returns the unix epoch timestamp (UTC, start of hour).
    """
    match = FILENAME_PATTERN.match(filename)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {filename}")
    year, month, day, hour = map(int, match.groups())
    dt = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)
    return int(dt.timestamp())

def main():

    dfs = []
    for fname in sorted(os.listdir(DATA_DIR)):
        if fname.endswith(".parquet") and FILENAME_PATTERN.match(fname):
            fpath = os.path.join(DATA_DIR, fname)
            df = pd.read_parquet(fpath)
            timestamp = extract_timestamp_from_filename(fname)
            df["timestamp"] = timestamp
            # Use h3.int_to_str for canonical H3 string, handle NaN safely
            df["H3_7_STR"] = df["h3_7"].apply(lambda x: h3.int_to_str(int(x)) if pd.notnull(x) else None)
            dfs.append(df)
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_parquet(OUTPUT_FILE, index=False)
    print(f"Merged {len(dfs)} files into {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 