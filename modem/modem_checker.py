#!/usr/bin/env python3

import subprocess
import time

# Configuration
CHECK_INTERVAL = 1  # Seconds
USB_PORT = "1"      # Usually port 2 on RPi 4/5, use 'uhubctl' to verify

def is_modem_present():
    try:
        # Run mmcli -L and capture output
        result = subprocess.run(['mmcli', '-L'], capture_output=True, text=True)
        # If "No modems were found" is in output or it's empty, it's missing
        if "No modems were found" in result.stdout or not result.stdout.strip():
            return False
        return True
    except Exception as e:
        print(f"Error checking modem: {e}")
        return False

def reset_usb_power():
    print("Modem missing! Resetting USB power...")
    try:
        # Turn off power
        subprocess.run(['uhubctl', '-l', '1-1', '-p', USB_PORT, '-a', '0'], check=True)
        time.sleep(2) # Give it a moment to fully discharge
        # Turn on power
        subprocess.run(['uhubctl', '-l', '1-1', '-p', USB_PORT, '-a', '1'], check=True)
        print("Power cycled. Waiting for modem to initialize...")
        time.sleep(10) # Wait for the modem to boot before checking again
    except Exception as e:
        print(f"Failed to reset USB: {e}")

def main():
    print("Starting Modem Watchdog...")
    while True:
        if not is_modem_present():
            reset_usb_power()
        else:
            # Optional: print status
            pass
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
