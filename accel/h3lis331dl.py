#!/usr/bin/env python3
"""
H3LIS331DL Dual High-G Accelerometer Logger
Binary output, per-sensor files, 5MB rotation with atomic rename.
Optional TCP live stream for field validation.
Systemd watchdog integration for ghost process detection.

Usage:
  python3 h3lis331dl.py <i2c_bus_number> [tcp_port]
  python3 h3lis331dl.py 0
  python3 h3lis331dl.py 0 6000
  python3 h3lis331dl.py 3 6001

Sensors per bus (auto-detected):
  0x19 (SA0 high) and 0x18 (SA0 low)

Output files:
  /dev/shm/raw/accel_bus0_0x19_1770773050.dat.tmp  (writing)
  /dev/shm/raw/accel_bus0_0x19_1770773050.dat      (sealed, ready for analysis)

Live stream (when port specified and client connected):
  nc localhost 6000
  Output: timestamp,bus,addr,accel_x_g,accel_y_g,accel_z_g

Service file requires:
  Type=notify
  WatchdogSec=30

Binary format:
  Header (22 bytes):
    4B  magic       "ACLB"
    1B  version     1
    1B  bus         i2c bus number
    1B  addr        sensor address
    1B  full_scale  200 (+-200g)
    2B  sample_rate uint16 (1000)
    4B  sensitivity float32 (0.098)
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
import socket
import fcntl

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

# +-200g, 1000Hz, BDU
CTRL_REG1_VAL = 0x3F
CTRL_REG4_VAL = 0x90          # BDU=1, FS=01 (+-200g)  — was 0x80 for +-100g
SENSITIVITY   = 0.098         # g/digit at +-200g       — was 0.049 for +-100g
FULL_SCALE    = 200           #                         — was 100
SAMPLE_RATE_HZ = 1000
SAMPLE_INTERVAL = 1.0 / SAMPLE_RATE_HZ

# Binary format
FILE_MAGIC      = b"ACLB"
FILE_VERSION    = 1
HEADER_FORMAT   = "<4sBBBBHfd"
HEADER_SIZE     = struct.calcsize(HEADER_FORMAT)  # 22 bytes
RECORD_FORMAT   = "<dfff"
RECORD_SIZE     = struct.calcsize(RECORD_FORMAT)   # 20 bytes

# File output
OUTPUT_DIR      = "/dev/shm/raw"
MAX_FILE_BYTES  = 5 * 1024 * 1024  # 5MB per file

# Error handling
MAX_CONSECUTIVE_ERRORS = 50
ERROR_COOLDOWN_SEC     = 0.5
I2C_RESET_COOLDOWN_SEC = 3.0

# I2C kernel timeout — prevents process from going into indefinite D-state
# ioctl I2C_TIMEOUT = 0x0702, value in 10ms units (kernel jiffies)
I2C_TIMEOUT_IOCTL    = 0x0702
I2C_TIMEOUT_JIFFIES  = 100   # 1 second max wait per I2C transaction

# Live stream
ACCEPT_CHECK_INTERVAL  = 100  # check for new client every N samples

# Watchdog — ping systemd every ~1 second
WATCHDOG_INTERVAL = 1000  # every N samples

# Logging
LOG_DIR = "/tmp"


# --------- SYSTEMD WATCHDOG ---------

def sd_notify(msg):
    """Send notification to systemd. Silent no-op if not under systemd."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            if addr[0] == "@":
                addr = "\0" + addr[1:]
            sock.sendto(msg.encode(), addr)
        finally:
            sock.close()
    except Exception:
        pass


def watchdog_ping():
    """Tell systemd we are still alive."""
    sd_notify("WATCHDOG=1")


def sd_ready():
    """Tell systemd the service has started successfully."""
    sd_notify("READY=1")


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
            try:
                self.fd.close()
            except Exception:
                pass
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
            try:
                self.fd.flush()
            except Exception as e:
                self.log.error("Flush error: %s", e)
                self._seal_file()

        # Rotate if file is full
        if self.bytes_written >= MAX_FILE_BYTES:
            self._seal_file()

    def close(self):
        """Seal any open file on shutdown."""
        self._seal_file()


# --------- LIVE STREAM ---------

class LiveStream:
    """Non-blocking TCP server for optional live CSV streaming.
    Zero overhead when no client is connected.
    One client at a time, best-effort delivery."""

    def __init__(self, port, bus_num, log):
        self.port = port
        self.bus_num = bus_num
        self.log = log
        self.server_sock = None
        self.client_sock = None
        self.client_addr = None

    def start(self):
        """Bind and listen. Returns True on success."""
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_sock.setblocking(False)
            self.server_sock.bind(("0.0.0.0", self.port))
            self.server_sock.listen(1)
            self.log.info("Live stream listening on port %d", self.port)
            return True
        except Exception as e:
            self.log.error("Failed to start live stream on port %d: %s",
                           self.port, e)
            if self.server_sock is not None:
                try:
                    self.server_sock.close()
                except Exception:
                    pass
            self.server_sock = None
            return False

    def check_accept(self):
        """Non-blocking check for new client. Call periodically."""
        if self.server_sock is None:
            return
        try:
            conn, addr = self.server_sock.accept()
            if self.client_sock is not None:
                try:
                    conn.sendall(b"ERROR: another client is connected\n")
                    conn.close()
                except Exception:
                    pass
                return
            conn.setblocking(False)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.client_sock = conn
            self.client_addr = addr
            self.log.info("Live stream client connected: %s:%d",
                          addr[0], addr[1])
        except BlockingIOError:
            pass
        except Exception:
            pass

    def send_sample(self, ts, addr, ax, ay, az):
        """Send one CSV line to the connected client.
        Returns False if client was dropped."""
        if self.client_sock is None:
            return True
        line = "%.4f,%d,0x%02x,%.3f,%.3f,%.3f\n" % (
            ts, self.bus_num, addr, ax, ay, az)
        try:
            self.client_sock.sendall(line.encode())
            return True
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self._drop_client("disconnected")
            return False
        except BlockingIOError:
            self._drop_client("too slow")
            return False
        except OSError:
            self._drop_client("connection error")
            return False
        except Exception:
            self._drop_client("unexpected error")
            return False

    def has_client(self):
        """Check if a client is currently connected."""
        return self.client_sock is not None

    def _drop_client(self, reason):
        """Clean up client connection."""
        if self.client_sock is not None:
            addr_str = "%s:%d" % (self.client_addr[0], self.client_addr[1]) \
                if self.client_addr else "unknown"
            self.log.info("Live stream client %s: %s", addr_str, reason)
            try:
                self.client_sock.close()
            except Exception:
                pass
            self.client_sock = None
            self.client_addr = None

    def close(self):
        """Shut down server and client."""
        self._drop_client("shutdown")
        if self.server_sock is not None:
            try:
                self.server_sock.close()
            except Exception:
                pass
            self.server_sock = None


# --------- MAIN LOGGER ---------

class AccelLogger:
    def __init__(self, i2c_bus, tcp_port=None):
        self.i2c_bus = i2c_bus
        self.tcp_port = tcp_port
        self.bus = None
        self.running = True
        self.sample_count = 0
        self.error_count = 0
        self.consecutive_errors = 0
        self.active_sensors = []
        self.sensor_files = {}
        self.live_stream = None

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
                self.bus = None
            self.bus = smbus2.SMBus(self.i2c_bus)
            time.sleep(0.01)

            # Set kernel-level I2C timeout to prevent indefinite D-state.
            # Without this, a stuck sensor can block the read_i2c_block_data()
            # call forever, putting the process into uninterruptible sleep.
            try:
                fcntl.ioctl(self.bus.fd, I2C_TIMEOUT_IOCTL,
                            I2C_TIMEOUT_JIFFIES)
                self.log.info("I2C timeout set to %dms",
                              I2C_TIMEOUT_JIFFIES * 10)
            except Exception as e:
                self.log.warning("Could not set I2C timeout: %s "
                                 "(process may D-state on stuck sensor)", e)

            return True
        except Exception as e:
            self.log.error("Failed to open i2c-%d: %s", self.i2c_bus, e)
            self.bus = None
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
            self.log.info("[%s] 0x%02x: ready (+-200g, 1000Hz)", label, addr)
            return True
        except OSError as e:
            self.log.warning("[%s] 0x%02x: not found (%s)", label, addr, e)
            return False
        except Exception as e:
            self.log.warning("[%s] 0x%02x: init failed (%s)", label, addr, e)
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
            watchdog_ping()
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
        self.log.info("Starting (i2c-%d, +-200g, %dHz, binary output)",
                      self.i2c_bus, SAMPLE_RATE_HZ)
        self.log.info("Record: %dB header + %dB/sample, rotate at %dMB",
                      HEADER_SIZE, RECORD_SIZE, MAX_FILE_BYTES // (1024 * 1024))

        if not self._ensure_output_dir():
            self.log.error("Cannot create output dir, exiting")
            sys.exit(1)

        # Start live stream if port specified
        if self.tcp_port is not None:
            self.live_stream = LiveStream(self.tcp_port, self.i2c_bus, self.log)
            if not self.live_stream.start():
                self.log.warning("Live stream unavailable, continuing without it")
                self.live_stream = None

        # Init sensors with retry
        while self.running:
            watchdog_ping()
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

        # Notify systemd we are ready and alive
        sd_ready()
        watchdog_ping()

        last_status_time = time.time()
        next_sample_time = time.time()
        loop_count = 0

        while self.running:
            try:
                now = time.time()
                sleep_time = next_sample_time - now
                if sleep_time > 0:
                    time.sleep(sleep_time)

                ts = time.time()
                next_sample_time = ts + SAMPLE_INTERVAL

                # Watchdog ping every ~1 second
                if loop_count % WATCHDOG_INTERVAL == 0:
                    watchdog_ping()

                # Check for live stream client periodically
                if self.live_stream is not None and \
                        loop_count % ACCEPT_CHECK_INTERVAL == 0:
                    self.live_stream.check_accept()

                streaming = self.live_stream is not None and \
                    self.live_stream.has_client()

                for s in self.active_sensors:
                    ax, ay, az = read_accel(self.bus, s["addr"])
                    self.sensor_files[s["addr"]].write_sample(ts, ax, ay, az)

                    if streaming:
                        self.live_stream.send_sample(
                            ts, s["addr"], ax, ay, az)

                self.sample_count += 1
                self.consecutive_errors = 0
                loop_count += 1

                # Status every 5 minutes
                if now - last_status_time >= 300:
                    file_info = []
                    for s in self.active_sensors:
                        sf = self.sensor_files[s["addr"]]
                        file_info.append("0x%02x:%dKB" % (
                            s["addr"],
                            sf.bytes_written // 1024 if sf.fd else 0))
                    stream_status = ""
                    if self.live_stream is not None:
                        if self.live_stream.has_client():
                            stream_status = ", stream=active"
                        else:
                            stream_status = ", stream=idle"
                    self.log.info("OK: %d samples, %d errors, files=[%s]%s",
                                  self.sample_count, self.error_count,
                                  ", ".join(file_info), stream_status)
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
        except Exception as e:
            self.log.error("File cleanup error: %s", e)
        try:
            if self.live_stream is not None:
                self.live_stream.close()
        except Exception as e:
            self.log.error("Stream cleanup error: %s", e)
        try:
            if self.bus:
                for s in self.active_sensors:
                    try:
                        self.bus.write_byte_data(s["addr"], CTRL_REG1, 0x07)
                    except Exception:
                        pass
                self.bus.close()
        except Exception as e:
            self.log.error("Bus cleanup error: %s", e)
        self.log.info("Stopped (%d samples, %d errors)",
                      self.sample_count, self.error_count)


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: h3lis331dl.py <i2c_bus_number> [tcp_port]")
        sys.exit(1)

    try:
        bus_num = int(sys.argv[1])
    except ValueError:
        print("Error: bus number must be an integer")
        sys.exit(1)

    tcp_port = None
    if len(sys.argv) == 3:
        try:
            tcp_port = int(sys.argv[2])
            if tcp_port < 1 or tcp_port > 65535:
                print("Error: port must be 1-65535")
                sys.exit(1)
        except ValueError:
            print("Error: port must be an integer")
            sys.exit(1)

    logger = AccelLogger(bus_num, tcp_port)
    try:
        logger.run()
    except Exception as e:
        logger.log.critical("FATAL: %s", e)
        logger.log.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
