#!/usr/bin/env python3

import subprocess
import json
import gzip
import requests
import sys
import os
import fcntl
import argparse
import glob
from datetime import datetime

# --- Configuration ---
POST_URL = "https://www.luceon.us/test_post.php"
# --- End Configuration ---

def log(message):
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} > {message}")

def get_modem_imei_signal():
    """Retrieves modem IMEI and signal; returns defaults on failure."""
    command = ["mmcli", "-m", "0", "-J"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        modem_data = json.loads(result.stdout)
        generic = modem_data.get('modem', {}).get('generic', {})
        imei = generic.get('equipment-identifier')
        signal = generic.get('signal-quality', {}).get('value')
        return [imei if imei else "Unknown", signal if signal else "0"]
    except Exception as e:
        log(f"Warning: Modem check failed: {e}")
        return ["Unknown", "0"]

def gzip_post_and_remove(file_path, url, data_type, imei, signal):
    """Processes a single file: Gzip -> POST -> Delete."""
    try:
        if not os.path.exists(file_path):
            return False

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            log(f"Skipping empty file: {os.path.basename(file_path)}")
            # Optionally remove empty files here if desired
            return True

        log(f"Processing: {os.path.basename(file_path)} ({file_size} bytes)")

        with open(file_path, 'rb') as f_in:
            file_data = f_in.read()

        compressed_data = gzip.compress(file_data)

        headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/octet-stream',
            'User-Agent': 'python-gzip-uploader/1.0'
        }

        params = {
            'type': data_type,
            'imei': imei,
            'signal': signal,
            'filename': os.path.basename(file_path)
        }

        response = requests.post(url, params=params, data=compressed_data, headers=headers, timeout=900)
        response.raise_for_status()

        # 7. REMOVAL: Only occurs if response was successful (2xx)
        os.remove(file_path)
        log(f"Successfully uploaded and DELETED {os.path.basename(file_path)}")
        return True

    except Exception as e:
        log(f"Failed to process {file_path}: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gzip and upload files matching a pattern, then delete them.")
    parser.add_argument("pattern", type=str, help="File pattern (e.g., '/dev/shm/accel*.dat')")
    parser.add_argument("type", type=str, help="Data type label")
    args = parser.parse_args()

    lock_path = f"/tmp/sender_{args.type}.lock"

    # Open the lock file
    try:
        lock_file_fd = open(lock_path, 'w')
        fcntl.flock(lock_file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        files = glob.glob(args.pattern)
        if not files:
            log(f"No files found matching pattern: {args.pattern}")
            sys.exit(0)

        log(f"--- Starting Batch: {len(files)} files ---")
        imei, signal = get_modem_imei_signal()

        success_count = 0
        for f_path in files:
            if gzip_post_and_remove(f_path, POST_URL, args.type, imei, signal):
                success_count += 1

        log(f"Batch complete. {success_count}/{len(files)} files removed.")

    except IOError:
        log(f"Another instance for '{args.type}' is already running.")
        sys.exit(1)
    finally:
        # Cleanup lock
        if 'lock_file_fd' in locals():
            fcntl.flock(lock_file_fd, fcntl.LOCK_UN)
            lock_file_fd.close()
