#!/usr/bin/env python3

import pandas as pd
import sys
import argparse
from datetime import datetime

def find_highest_sustained_impact(file_path, window_ms):
    # Load the CSV
    column_names = ['timestamp', 'bus_id', 'sensor_id', 'x', 'y', 'z']
    try:
        df = pd.read_csv(file_path, names=column_names)
    except FileNotFoundError:
        return None

    # Convert timestamp to datetime and set as index for time-based rolling
    df['dt'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.sort_values('dt').set_index('dt')

    axes = ['x', 'y', 'z']
    df_abs = df[axes].abs()

    highest_impact = 0
    impact_details = None

    for axis in axes:
        # Rolling minimum ensures the value was SUSTAINED for the duration
        window_str = f'{window_ms}ms'
        sustained_values = df_abs[axis].rolling(window=window_str).min()

        max_val = sustained_values.max()

        if max_val > highest_impact:
            highest_impact = max_val
            end_time = sustained_values.idxmax()
            impact_details = {
                'axis': axis,
                'value': max_val,
                'end_time': end_time,
                'bus_id': df.loc[end_time, 'bus_id'],
                'sensor_id': df.loc[end_time, 'sensor_id'],
                'timestamp': df.loc[end_time, 'timestamp']
            }

    return impact_details

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect sustained impacts in accelerometer data.")
    parser.add_argument("input", help="Path to the input CSV file")
    parser.add_argument("ms", type=int, help="Sustained duration in milliseconds")
    parser.add_argument("output", help="Path to the output log file")

    args = parser.parse_args()

    result = find_highest_sustained_impact(args.input, args.ms)

    # Open output file in append mode ('a')
    with open(args.output, "a") as f:
        print(f"\n--- Analysis Run: {datetime.now()} ---\n")
        print(f"Input File: {args.input} | Window: {args.ms}ms\n")

        if result:
            f.write(f"{result['timestamp']},{result['bus_id']},{result['sensor_id']},{result['axis']},{result['value']:.4f}\n")
            print(f"Analysis complete. Result appended to {args.output}")
        else:
            print("No data found.\n")
