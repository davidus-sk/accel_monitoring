#!/usr/bin/env python3
"""
H3LIS331DL Dual High-G Accelerometer Logger
Binary output, per-sensor files, 5MB rotation with atomic rename.

Usage:
  python3 h3lis331dl.py <i2c_bus_number>
  python3 h3lis331dl.py 0
  python3 h3lis331dl.py 3

Sensors per bus (auto-detected):
  0x19 (SA0 high) and 0x18 (SA0 low)

Output files:
  /dev/shm/raw/accel_bus0_0x19_1770773050.dat.tmp  (writing)
  /dev/shm/raw/accel_bus0_0x19_1770773050.dat      (sealed, ready for analysis)

Binary format:
  Header (22 bytes):
    4B  magic       "ACLB"
    1B  version     1
    1B  bus         i2c bus number
    1B  addr        sensor address
    1B  full_scale  100 (+-100g)
    2B  sample_rate uint16 (1000)
    4B  sensitivity float32 (0.049)
    8B  start_ts    float64 (unix timestamp of first sample)

  Records (20 bytes each):
    8B  timestamp   float64 (unix epoch)
    4B  accel_x     float32 (g)
    4B  accel_y     float32 (g)
    4B  accel_z     float32 (g)
"""

import smbus2
import struct
import time
import sys
import signal
import traceback
import os
import logging

# --------- CONFIGURATION ---------

SENSORS = [
    {"label": "A", "addr": 0x19},
    {"label": "B", "addr": 0x18},
]

# Registers
WHO_AM_I   = 0x0F
CTRL_REG1  = 0x20
CTRL_REG2  = 0x21
CTRL_REG3  = 0x22
CTRL_REG4  = 0x23
OUT_X_L    = 0x28

EXPECTED_WHO_AM_I = 0x32

# +-100g, 1000Hz, BDU
CTRL_REG1_VAL = 0x3F
CTRL_REG4_VAL = 0x80
SENSITIVITY   = 0.049
FULL_SCALE    = 100
SAMPLE_RATE_HZ = 1000
SAMPLE_INTERVAL = 1.0 / SAMPLE_RATE_HZ

# Binary format
FILE_MAGIC      = b"ACLB"
FILE_VERSION    = 1
HEADER_FORMAT   = "<4sBBBBHfd"   # magic(4) ver(1) bus(1) addr(1) fs(1) rate(2) sens(4) start_ts(8)
HEADER_SIZE     = struct.calcsize(HEADER_FORMAT)  # 22 bytes
RECORD_FORMAT   = "<dfff"        # timestamp(8) ax(4) ay(4) az(4)
RECORD_SIZE     = struct.calcsize(RECORD_FORMAT)   # 20 bytes

# File output
OUTPUT_DIR      = "/dev/shm/raw"
MAX_FILE_BYTES  = 5 * 1024 * 1024  # 5MB per file

# Error handling
MAX_CONSECUTIVE_ERRORS = 50
ERROR_COOLDOWN_SEC     = 0.5
I2C_RESET_COOLDOWN_SEC = 3.0

# Logging
LOG_DIR = "/tmp"


# --------- SENSOR READ ---------

def read_accel(bus, addr):
    """Read 3-axis accel. Returns (ax, ay, az) in g."""
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


# --------- PER-SENSOR FILE WRITER ---------

class SensorFile:
    """Manages binary file output for one sensor with rotation and atomic rename."""

    def __init__(self, bus_num, addr, log):
        self.bus_num = bus_num
        self.addr = addr
        self.log = log
        self.fd = None
        self.tmp_path = None
        self.final_path = None
        self.bytes_written = 0
        self.samples_in_file = 0

    def _new_file(self, start_ts):
        """Open a new .tmp file and write the header."""
        ts_int = int(start_ts)
        base = f"accel_bus{self.bus_num}_0x{self.addr:02x}_{ts_int}"
        self.tmp_path = os.path.join(OUTPUT_DIR, base + ".dat.tmp")
        self.final_path = os.path.join(OUTPUT_DIR, base + ".dat")

        self.fd = open(self.tmp_path, "wb")
        header = struct.pack(
            HEADER_FORMAT,
            FILE_MAGIC,
            FILE_VERSION,
            self.bus_num,
            self.addr,
            FULL_SCALE,
            SAMPLE_RATE_HZ,
            SENSITIVITY,
            start_ts
        )
        self.fd.write(header)
        self.bytes_written = HEADER_SIZE
        self.samples_in_file = 0

    def _seal_file(self):
        """Flush, close, and atomically rename .tmp to .dat."""
        if self.fd is None:
            return
        try:
            self.fd.flush()
            os.fsync(self.fd.fileno())
            self.fd.close()
        except Exception as e:
            self.log.error("Error closing %s: %s", self.tmp_path, e)
            self.fd = None
            return

        self.fd = None
        try:
            os.rename(self.tmp_path, self.final_path)
        except Exception as e:
            self.log.error("Error renaming %s -> %s: %s",
                           self.tmp_path, self.final_path, e)

    def write_sample(self, ts, ax, ay, az):
        """Write one 20-byte record. Rotates file if size exceeded."""
        if self.fd is None:
            self._new_file(ts)

        record = struct.pack(RECORD_FORMAT, ts, ax, ay, az)
        self.fd.write(record)
        self.bytes_written += RECORD_SIZE
        self.samples_in_file += 1

        # Flush periodically (every 100 samples)
        if self.samples_in_file % 100 == 0:
            self.fd.flush()

        # Rotate if file is full
        if self.bytes_written >= MAX_FILE_BYTES:
            self._seal_file()

    def close(self):
        """Seal any open file on shutdown."""
        self._seal_file()


# --------- MAIN LOGGER ---------

class AccelLogger:
    def __init__(self, i2c_bus):
        self.i2c_bus = i2c_bus
        self.bus = None
        self.running = True
        self.sample_count = 0
        self.error_count = 0
        self.consecutive_errors = 0
        self.active_sensors = []
        self.sensor_files = {}

        log_path = os.path.join(LOG_DIR, f"accel_bus{i2c_bus}.log")
        self.log = logging.getLogger(f"accel_bus{i2c_bus}")
        self.log.setLevel(logging.INFO)

        fh = logging.FileHandler(log_path)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
        self.log.addHandler(fh)

        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
        self.log.addHandler(sh)

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.log.info("Signal %d received, stopping", signum)
        self.running = False

    def _ensure_output_dir(self):
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            return True
        except Exception as e:
            self.log.error("Failed to create %s: %s", OUTPUT_DIR, e)
            return False

    def open_bus(self):
        try:
            if self.bus:
                try:
                    self.bus.close()
                except Exception:
                    pass
            self.bus = smbus2.SMBus(self.i2c_bus)
            time.sleep(0.01)
            return True
        except Exception as e:
            self.log.error("Failed to open i2c-%d: %s", self.i2c_bus, e)
            return False

    def init_sensor(self, addr, label):
        try:
            who = self.bus.read_byte_data(addr, WHO_AM_I)
            if who != EXPECTED_WHO_AM_I:
                self.log.warning("[%s] 0x%02x: unexpected WHO_AM_I=0x%02x",
                                 label, addr, who)
                return False
            self.bus.write_byte_data(addr, CTRL_REG1, CTRL_REG1_VAL)
            self.bus.write_byte_data(addr, CTRL_REG4, CTRL_REG4_VAL)
            self.bus.write_byte_data(addr, CTRL_REG2, 0x00)
            self.bus.write_byte_data(addr, CTRL_REG3, 0x00)
            time.sleep(0.005)
            self.log.info("[%s] 0x%02x: ready (+-100g, 1000Hz)", label, addr)
            return True
        except OSError as e:
            self.log.warning("[%s] 0x%02x: not found (%s)", label, addr, e)
            return False

    def init_all_sensors(self):
        if not self.open_bus():
            return []
        active = []
        for s in SENSORS:
            if self.init_sensor(s["addr"], s["label"]):
                active.append(s)
        return active

    def _open_sensor_files(self):
        """Create a SensorFile for each active sensor."""
        # Close any existing files
        for sf in self.sensor_files.values():
            sf.close()
        self.sensor_files = {}
        for s in self.active_sensors:
            self.sensor_files[s["addr"]] = SensorFile(
                self.i2c_bus, s["addr"], self.log)

    def recover(self):
        self.log.info("Recovery started")
        self.consecutive_errors = 0
        time.sleep(I2C_RESET_COOLDOWN_SEC)

        while self.running:
            self.active_sensors = self.init_all_sensors()
            if self.active_sensors:
                break
            self.log.error("No sensors found, retrying in %ds",
                           int(I2C_RESET_COOLDOWN_SEC))
            time.sleep(I2C_RESET_COOLDOWN_SEC)

        if self.running:
            self._open_sensor_files()
            self.log.info("Recovery complete, %d sensor(s)",
                          len(self.active_sensors))

    def run(self):
        self.log.info("Starting (i2c-%d, +-100g, %dHz, binary output)",
                      self.i2c_bus, SAMPLE_RATE_HZ)
        self.log.info("Record: %dB header + %dB/sample, rotate at %dMB",
                      HEADER_SIZE, RECORD_SIZE, MAX_FILE_BYTES // (1024 * 1024))

        if not self._ensure_output_dir():
            self.log.error("Cannot create output dir, exiting")
            sys.exit(1)

        # Init sensors with retry
        while self.running:
            self.active_sensors = self.init_all_sensors()
            if self.active_sensors:
                break
            self.log.error("No sensors on startup, retrying in %ds",
                           int(I2C_RESET_COOLDOWN_SEC))
            time.sleep(I2C_RESET_COOLDOWN_SEC)

        if not self.running:
            return

        self._open_sensor_files()
        self.log.info("Sampling %d sensor(s)", len(self.active_sensors))

        last_status_time = time.time()
        next_sample_time = time.time()

        while self.running:
            try:
                now = time.time()
                sleep_time = next_sample_time - now
                if sleep_time > 0:
                    time.sleep(sleep_time)

                ts = time.time()
                next_sample_time = ts + SAMPLE_INTERVAL

                for s in self.active_sensors:
                    ax, ay, az = read_accel(self.bus, s["addr"])
                    self.sensor_files[s["addr"]].write_sample(ts, ax, ay, az)

                self.sample_count += 1
                self.consecutive_errors = 0

                # Status every 5 minutes
                if now - last_status_time >= 300:
                    file_info = []
                    for s in self.active_sensors:
                        sf = self.sensor_files[s["addr"]]
                        file_info.append("0x%02x:%dKB" % (
                            s["addr"],
                            sf.bytes_written // 1024 if sf.fd else 0))
                    self.log.info("OK: %d samples, %d errors, files=[%s]",
                                  self.sample_count, self.error_count,
                                  ", ".join(file_info))
                    last_status_time = now

            except IOError as e:
                self.error_count += 1
                self.consecutive_errors += 1
                if self.consecutive_errors == 1:
                    self.log.error("I2C error: %s", e)
                if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    self.log.error("%d consecutive I2C errors, recovering",
                                   self.consecutive_errors)
                    self.recover()
                else:
                    time.sleep(ERROR_COOLDOWN_SEC)

            except Exception as e:
                self.error_count += 1
                self.consecutive_errors += 1
                self.log.error("Unexpected: %s", e)
                if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    self.log.error("%d consecutive errors, full recovery",
                                   self.consecutive_errors)
                    self.recover()
                else:
                    time.sleep(ERROR_COOLDOWN_SEC)

        self.cleanup()

    def cleanup(self):
        self.log.info("Shutting down...")
        try:
            for sf in self.sensor_files.values():
                sf.close()
            if self.bus:
                for s in self.active_sensors:
                    try:
                        self.bus.write_byte_data(s["addr"], CTRL_REG1, 0x07)
                    except Exception:
                        pass
                self.bus.close()
        except Exception as e:
            self.log.error("Cleanup error: %s", e)
        self.log.info("Stopped (%d samples, %d errors)",
                      self.sample_count, self.error_count)


def main():
    if len(sys.argv) != 2:
        print("Usage: h3lis331dl.py <i2c_bus_number>")
        sys.exit(1)

    try:
        bus_num = int(sys.argv[1])
    except ValueError:
        print("Error: bus number must be an integer")
        sys.exit(1)

    logger = AccelLogger(bus_num)
    try:
        logger.run()
    except Exception as e:
        logger.log.critical("FATAL: %s", e)
        logger.log.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
