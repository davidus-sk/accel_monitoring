#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
from datetime import datetime

def estimate_deflection(magnitudes, timestamps_ms):
    """
    Estimates displacement using double trapezoidal integration.
    magnitudes: array of acceleration (m/s^2)
    timestamps_ms: array of timestamps in milliseconds
    """
    # 1. Convert ms to seconds and magnitude to m/s^2
    # (Assuming magnitude is in Gs, multiply by 9.81)
    t = np.array(timestamps_ms)
    accel = np.array(magnitudes) * 9.81

    # Remove gravity if the sensor wasn't zeroed (approximate)
    accel = accel - np.mean(accel)

    # 2. First integration: Acceleration -> Velocity
    # v(t) = integral of a(t)
    velocity = np.zeros_like(accel)
    for i in range(1, len(accel)):
        dt = t[i] - t[i-1]
        velocity[i] = velocity[i-1] + (accel[i] + accel[i-1]) / 2 * dt

    # 3. Second integration: Velocity -> Displacement (Deflection)
    # d(t) = integral of v(t)
    displacement = np.zeros_like(velocity)
    for i in range(1, len(velocity)):
        dt = t[i] - t[i-1]
        displacement[i] = displacement[i-1] + (velocity[i] + velocity[i-1]) / 2 * dt

    # Return the peak-to-peak displacement (the "deflection" range)
    return np.max(displacement) - np.min(displacement)

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

    # Calculate Magnitude: sqrt(x^2 + y^2 + z^2)
    df['magnitude'] = np.sqrt(df['x']**2 + df['y']**2 + df['z']**2)

    # Calculate the rolling minimum of the magnitude over the time window.
    # This identifies the highest level that the magnitude NEVER dropped
    # below during the Xms duration.
    window_str = f'{window_ms}ms'
    sustained_magnitude = df['magnitude'].rolling(window=window_str).min()

    max_val = sustained_magnitude.max()
    median = df['magnitude'].mean()

    if pd.isna(max_val):
        return None

    # Identify the end of the window where this max sustained magnitude occurred
    end_time = sustained_magnitude.idxmax()

    # deflection
    impact_start_time = end_time - pd.Timedelta(milliseconds=50)
    impact_end_time = end_time + pd.Timedelta(milliseconds=50)
    impact_window = df.loc[impact_start_time:impact_end_time]
    deflection = estimate_deflection(impact_window['magnitude'], impact_window['timestamp'])

    return {
        'value': max_val,
        'median': median,
        'end_time': end_time,
        'bus_id': df.loc[end_time, 'bus_id'],
        'sensor_id': df.loc[end_time, 'sensor_id'],
        'timestamp': df.loc[end_time, 'timestamp'],
        'deflection': deflection
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect sustained impacts in accelerometer data.")
    parser.add_argument("input", help="Path to the input CSV file")
    parser.add_argument("ms", type=float, help="Sustained duration in milliseconds")
    parser.add_argument("output", help="Path to the output log file")

    args = parser.parse_args()

    result = find_highest_sustained_impact(args.input, args.ms)

    # Open output file in append mode ('a')
    with open(args.output, "a") as f:
        print(f"\n--- Analysis Run: {datetime.now()} ---\n")
        print(f"Input File: {args.input} | Window: {args.ms}ms\n")

        if result:
            f.write(f"{result['timestamp']},{result['bus_id']},{result['sensor_id']},{result['value']:.4f},{result['median']:.4f},{result['deflection']:.4f}\n")
            print(f"Analysis complete. Result appended to {args.output}")
        else:
            print("No data found.\n")
