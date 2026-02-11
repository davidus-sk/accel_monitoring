#!/usr/bin/env python3

import subprocess
import sys
import RPi.GPIO as GPIO
import time

def check_process_running(process_name):
    """
    Checks if a process with the given name is running using pgrep.
    Returns True if running, False otherwise.

    Note: This function assumes a Linux/UNIX-like environment
    where 'pgrep' is available.
    """
    try:
        # Run pgrep
        # check=True: Raises CalledProcessError if pgrep returns non-zero (not found)
        # capture_output=True: Suppresses stdout/stderr from pgrep
        subprocess.run(
            ["pgrep", "-f", process_name],
            check=True,
            capture_output=True
        )
        # If check=True passes, pgrep returned 0, meaning process was found
        return True
    except subprocess.CalledProcessError:
        # pgrep returned non-zero (exit code 1), meaning process not found
        return False
    except FileNotFoundError:
        # The 'pgrep' command itself was not found
        print(f"Error: 'pgrep' command not found.", file=sys.stderr)
        print("This script requires 'pgrep' to function.", file=sys.stderr)
        # Return False as we cannot determine process status
        return False
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred while checking for {process_name}: {e}", file=sys.stderr)
        return False

def check_executables(exe1, exe2):
    """
    Checks if two executables are running.

    Returns:
    - 2 if both are running
    - 1 if only one is running
    - 0 if neither is running
    """
    running_count = 0
    
    if check_process_running(exe1):
        print(f"Status: '{exe1}' is running.")
        running_count += 1
    else:
        print(f"Status: '{exe1}' is NOT running.")
        
    if check_process_running(exe2):
        print(f"Status: '{exe2}' is running.")
        running_count += 1
    else:
        print(f"Status: '{exe2}' is NOT running.")
        
    print(f"\nTotal processes found: {running_count}")
    return running_count

if __name__ == "__main__":

    # Set the pin mode (BCM or BOARD)
    GPIO.setmode(GPIO.BCM)

    # Set the GPIO pin as an output
    GPIO.setup(24, GPIO.OUT) # Example: using GPIO 17

    blink = False

    # --- IMPORTANT ---
    # For testing, change these names to processes
    # that are actually running or not running on your system.
    #
    # Good examples to test with:
    # - "systemd" (should always be running on modern Linux)
    # - "python3" (will be running when you execute this script)
    # - "a_fake_process_name_123" (should not be running)

    executable_a = "imu.py"
    executable_b = "temp.py"

    while True:
        print(f"Checking for '{executable_a}' and '{executable_b}'...")

        if blink:
            GPIO.output(24, GPIO.HIGH)

        status = check_executables(executable_a, executable_b)

        print(f"\nFunction returned: {status}")

        if status == 2:
            blink = True
            time.sleep(0.5)
            GPIO.output(24, GPIO.LOW)
            time.sleep(1)

        if status == 1:
            blink = True
            time.sleep(0.5)
            GPIO.output(24, GPIO.LOW)
            time.sleep(3)

        if status == 0:
            blink = False
            GPIO.output(24, GPIO.LOW)
            time.sleep(2)

    # --- Example test with known processes ---
    # print("\n" + "="*30)
    # print("Testing with 'systemd' and 'a_fake_process_123'...")
    # status_test = check_executables("systemd", "a_fake_process_123")
    # print(f"Test function returned: {status_test}")
    # print("="*30)
