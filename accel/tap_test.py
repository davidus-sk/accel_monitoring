#!/usr/bin/env python3
"""
Tap Detector - Identify which sensor is which by tapping/flicking.

Stop all collector services before running:
  sudo systemctl stop h3lis331dl-bus0.service h3lis331dl-bus3.service process-accel.service

Run:
  python3 tap_test.py

Tap or flick each sensor and note the output.
Press Ctrl+C to exit.
"""

import smbus2
import math
import time
import sys

WHO_AM_I      = 0x0F
CTRL_REG1     = 0x20
CTRL_REG4     = 0x23
OUT_X_L       = 0x28
EXPECTED_ID   = 0x32

# +-100g, 400Hz (lower ODR, we only need ~50Hz polling)
CTRL_REG1_VAL = 0x2F  # PM=001, DR=01 (400Hz), XYZ enabled
CTRL_REG4_VAL = 0x80  # BDU=1, +-100g
SENSITIVITY   = 0.049

TAP_THRESHOLD = 4.0   # g, vector magnitude
COOLDOWN_SEC  = 0.5   # suppress repeated prints per sensor

BUSES = [0, 3]
ADDRS = [0x19, 0x18]


def read_accel(bus, addr):
    data = bus.read_i2c_block_data(addr, OUT_X_L | 0x80, 6)
    raw_x = (data[1] << 8 | data[0])
    raw_y = (data[3] << 8 | data[2])
    raw_z = (data[5] << 8 | data[4])
    if raw_x >= 0x8000: raw_x -= 65536
    if raw_y >= 0x8000: raw_y -= 65536
    if raw_z >= 0x8000: raw_z -= 65536
    ax = (raw_x >> 4) * SENSITIVITY
    ay = (raw_y >> 4) * SENSITIVITY
    az = (raw_z >> 4) * SENSITIVITY
    return ax, ay, az


def main():
    print("Tap Detector - tap or flick each sensor to identify it")
    print("Press Ctrl+C to exit")
    print("-" * 50)

    # Open buses and init sensors
    active = []
    bus_handles = {}

    for bus_num in BUSES:
        try:
            b = smbus2.SMBus(bus_num)
            bus_handles[bus_num] = b
            time.sleep(0.01)
        except Exception:
            print("  i2c-%d: not available" % bus_num)
            continue

        for addr in ADDRS:
            try:
                who = b.read_byte_data(addr, WHO_AM_I)
                if who != EXPECTED_ID:
                    continue
                b.write_byte_data(addr, CTRL_REG1, CTRL_REG1_VAL)
                b.write_byte_data(addr, CTRL_REG4, CTRL_REG4_VAL)
                active.append((bus_num, addr))
                print("  Found: bus%d 0x%02x" % (bus_num, addr))
            except Exception:
                pass

    if not active:
        print("No sensors found.")
        sys.exit(1)

    # Flush stale data from previous session
    time.sleep(0.05)
    for bus_num, addr in active:
        try:
            read_accel(bus_handles[bus_num], addr)
        except Exception:
            pass
    time.sleep(0.05)

    print("")
    print("Listening on %d sensor(s)... tap away!" % len(active))
    print("")

    last_tap = {}
    for key in active:
        last_tap[key] = 0.0

    try:
        while True:
            now = time.time()
            for bus_num, addr in active:
                try:
                    ax, ay, az = read_accel(bus_handles[bus_num], addr)
                    mag = math.sqrt(ax * ax + ay * ay + az * az)
                    if mag >= TAP_THRESHOLD and (now - last_tap[(bus_num, addr)]) > COOLDOWN_SEC:
                        print("  TAP  bus%d  0x%02x  %.1fg" % (bus_num, addr, mag))
                        last_tap[(bus_num, addr)] = now
                except Exception:
                    pass
            time.sleep(0.02)  # ~50Hz polling

    except KeyboardInterrupt:
        print("")
        print("Done.")

    # Power down
    for bus_num, addr in active:
        try:
            bus_handles[bus_num].write_byte_data(addr, CTRL_REG1, 0x07)
        except Exception:
            pass
    for b in bus_handles.values():
        try:
            b.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
