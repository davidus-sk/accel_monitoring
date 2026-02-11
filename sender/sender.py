#!/usr/bin/env python3

import subprocess
import json
import gzip
import requests
import sys
import os
import fcntl
import argparse # Import the argparse module
from datetime import datetime

# --- Configuration ---
# The file to process
# SOURCE_FILE = "/dev/shm/imu.dat" # We will get this from the command line now

# IMPORTANT: Replace this with your actual endpoint URL
POST_URL = "https://www.luceon.us/test_post.php"
# --- End Configuration ---

def log(message):
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} > {message}")

def run_once_script(type):
    lock_file = f"/tmp/sender_{type}.lock" # Choose a suitable path
    try:
        with open(lock_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        log("Another instance of the script is already running. Exiting.")
        os._exit(1)

def get_modem_imei_signal():
    """
    Runs 'mmcli -m 0 -J' to get modem info as JSON and parses the
    connection status.
    """
    # The command to run, split into a list for subprocess
    command = ["mmcli", "-m", "0", "-J"]

    try:
        # Run the command
        # capture_output=True: Captures stdout and stderr
        # text=True: Decodes stdout and stderr as UTF-8 text
        # check=True: Raises CalledProcessError if the command returns non-zero
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8'
        )

        # The command succeeded, now parse the JSON output from stdout
        try:
            modem_data = json.loads(result.stdout)

            # Safely access the nested 'state' key.
            # .get('key', {}) returns the value or an empty dict if 'key' is missing.
            # This chain prevents KeyError exceptions if any part of the path is missing.
            imei = modem_data.get('modem', {}).get('generic', {}).get('equipment-identifier')
            signal = modem_data.get('modem', {}).get('generic', {}).get('signal-quality', {}).get('value')

            if imei:
                log(f"Successfully retrieved modem's IMEI.")
                log(f"IMEI: {imei}")
                return [imei, signal]
            else:
                log("Error: Could not find 'modem.generic.equipment-identifier' in the JSON output.")
                log(f"Raw output for debugging:\n{result.stdout}")
                return False

        except json.JSONDecodeError:
            log("Error: Failed to decode JSON from mmcli output.")
            log(f"Raw output for debugging:\n{result.stdout}")
            return False

    except FileNotFoundError:
        log(f"Error: Command '{command[0]}' not found.")
        log("Please ensure 'mmcli' (ModemManager) is installed and in your PATH.")
        return False

    except subprocess.CalledProcessError as e:
        # The command returned a non-zero exit code
        log(f"Error running command: {' '.join(command)}")
        log(f"Return Code: {e.returncode}")
        log(f"STDERR:\n{e.stderr}")
        log(f"STDOUT:\n{e.stdout}")
        log("This often means the modem (e.g., '-m 0') was not found.")
        return False

    except Exception as e:
        # Catch any other unexpected errors
        log(f"An unexpected error occurred: {e}")
        return False

def gzip_post_and_truncate(file_path, url, type, imei, signal):
    """
    Gzips a file, POSTs the compressed data to a URL, and truncates
    the original file if the POST is successful.
    """

    # 1. Check if source file exists
    if not os.path.exists(file_path):
        log(f"Error: Source file not found: {file_path}")
        return False

    # 2. Check if file is empty
    try:
        if os.path.getsize(file_path) == 0:
            log(f"Info: Source file is empty. Nothing to post. {file_path}")
            # File is already "empty", so no work is needed.
            # You could also choose to truncate it again, but this is fine.
            return True
    except OSError as e:
        log(f"Error: Could not get size of file: {e}")
        return False

    log(f"Reading and gzipping file: {file_path}")

    try:
        # 3. Read the file and compress it in memory
        with open(file_path, 'rb') as f_in:
            file_data = f_in.read()

        log(f"File size {len(file_data)} bytes");

        compressed_data = gzip.compress(file_data)

        # 4. Set up headers for the POST request
        headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/octet-stream', # Standard for binary data
            'User-Agent': 'python-gzip-uploader/1.0'
        }
        url = f"{url}?type={type}&imei={imei}&signal={signal}"
        log(f"Posting {len(compressed_data)} compressed bytes to {url}")

        # 5. Perform the POST request
        response = requests.post(
            url,
            data=compressed_data,
            headers=headers,
            timeout=900 # 30-second timeout
        )

        # 6. Check for successful response (e.g., 200 OK, 201 Created)
        # This will raise an exception for 4xx or 5xx client/server errors
        response.raise_for_status()

        log(f"Successfully posted data. Server responded with: {response.status_code}")

        # 7. Truncate the original file (only after successful POST)
        try:
            log(f"Truncating file: {file_path}")
            # Opening in 'w' (write) mode and immediately closing
            # is the standard way to truncate a file to 0.
            with open(file_path, 'w') as f_out:
                pass 
            log("File truncated.")
            return True

        except (IOError, PermissionError) as e:
            # This is a bad state: post succeeded but truncate failed.
            log(f"Error: POST succeeded, but FAILED to truncate file: {e}")
            return False

    except FileNotFoundError:
        # This could happen if file is deleted between the os.path.exists check
        # and the open() call (a race condition).
        log(f"Error: File disappeared during processing: {file_path}")
        return False
    except (IOError, PermissionError) as e:
        log(f"Error: Could not read file: {e}")
        return False
    except requests.exceptions.HTTPError as e:
        log(f"Error: HTTP error occurred: {e}")
        log(f"Response Body: {e.response.text}")
        return False
    except requests.exceptions.ConnectionError as e:
        log(f"Error: Connection error. Check network or URL: {e}")
        return False
    except requests.exceptions.Timeout:
        log(f"Error: Request timed out.")
        return False
    except requests.exceptions.RequestException as e:
        # Catch-all for other 'requests' library errors
        log(f"Error: An unexpected error occurred with the request: {e}")
        return False
    except Exception as e:
        # Catch any other unexpected errors
        lof(f"An unexpected error occurred: {e}")
        return False

if __name__ == "__main__":
    # --- Set up command-line argument parsing ---
    parser = argparse.ArgumentParser(
        description="Gzip a file, POST it to a URL, and truncate the file.",
        epilog=f"Example: {sys.argv[0]} /dev/shm/imu.dat"
    )
    # Add a required positional argument for the file path
    parser.add_argument(
        "filepath",
        type=str,
        help="The path to the source file to process."
    )
    parser.add_argument(
        "type",
        type=str,
        help="The type of data being posted to the server."
    )

    args = parser.parse_args()

    # --- End argument parsing ---

    log("-----------------------------------------------------")

    lock_file = f"/tmp/sender_{args.type}.lock"
    try:
        with open(lock_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

            data = get_modem_imei_signal()

            # Use the filepath from the parsed arguments
            if gzip_post_and_truncate(args.filepath, POST_URL, args.type, data[0], data[1]):
                log("Operation completed successfully.")
                sys.exit(0)
            else:
                log("Operation failed.")
                sys.exit(1)

    except IOError:
        log("Another instance of the script is already running. Exiting.")
        os._exit(1)
