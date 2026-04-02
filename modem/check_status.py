#!/usr/bin/env python3

import subprocess
import json
import sys
import RPi.GPIO as GPIO
import time

def get_modem_list():
    """Returns a list of modem indices found on the system."""
    try:
        result = subprocess.check_output(["mmcli", "-L", "-J"], stderr=subprocess.STDOUT)
        data = json.loads(result)
        # Extract indices from the 'modem-list' array
        return [m.split('/')[-1] for m in data.get("modem-list", [])]
    except Exception as e:
        print(f"Error listing modems: {e}")
        return []

def get_modem_status(index):
    """
    Runs 'mmcli -m 0 -J' to get modem info as JSON and parses the
    connection status.
    """
    # The command to run, split into a list for subprocess
    command = ["mmcli", "-m", index, "-J"]

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
            status = modem_data.get('modem', {}).get('generic', {}).get('state')

            if status:
                print(f"Successfully retrieved modem status.")
                print(f"Connection Status: {status}")
                return True
            else:
                print("Error: Could not find 'modem.generic.state' in the JSON output.")
                print("Raw output for debugging:\n", result.stdout)
                return False

        except json.JSONDecodeError:
            print("Error: Failed to decode JSON from mmcli output.", file=sys.stderr)
            print("Raw output for debugging:\n", result.stdout, file=sys.stderr)
            return False

    except FileNotFoundError:
        print(f"Error: Command '{command[0]}' not found.", file=sys.stderr)
        print("Please ensure 'mmcli' (ModemManager) is installed and in your PATH.", file=sys.stderr)
        return False

    except subprocess.CalledProcessError as e:
        # The command returned a non-zero exit code
        print(f"Error running command: {' '.join(command)}", file=sys.stderr)
        print(f"Return Code: {e.returncode}", file=sys.stderr)
        print(f"STDERR:\n{e.stderr}", file=sys.stderr)
        print(f"STDOUT:\n{e.stdout}", file=sys.stderr)
        print("This often means the modem (e.g., '-m 0') was not found.", file=sys.stderr)
        return False

    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":

    # Set the pin mode (BCM or BOARD)
    GPIO.setmode(GPIO.BCM)

    # Set the GPIO pin as an output
    GPIO.setup(23, GPIO.OUT) # Example: using GPIO 17

    while True:
        # Turn on
        GPIO.output(23, GPIO.HIGH)
        time.sleep(0.1)

        modem_list = get_modem_list()

        if list:
            status = get_modem_status(modem_list[0])

            if status:
                GPIO.output(23, GPIO.LOW)

        time.sleep(1)
