#!/usr/bin/env python3
"""
Accelerometer Event Processor

Reads sealed binary accel files from /dev/shm/raw/, detects anomaly events
using dual-threshold detection (absolute + relative), and writes event CSV
files to /dev/shm/ for GSM upload.

Detection logic:
  - Absolute threshold: vector magnitude > ABS_THRESHOLD_G
  - Relative threshold: vector magnitude > REL_MULTIPLIER * rolling_RMS
  - 5s context before and after each event
  - Events within MERGE_GAP_SEC are merged into one

Output CSV format (no header):
  timestamp,bus,addr,accel_x_g,accel_y_g,accel_z_g

Output filename:
  /dev/shm/accel_bus0_0x19_event_1770778924.dat
"""

import struct
import time
import sys
import signal
import os
import logging
import math
import traceback

# --------- CONFIGURATION ---------

RAW_DIR          = "/dev/shm/raw"
EVENT_DIR        = "/dev/shm"
LOG_PATH         = "/tmp/process_accel.log"

# Detection thresholds
ABS_THRESHOLD_G  = 10.0
REL_MULTIPLIER   = 4.0
RMS_WINDOW_SEC   = 2.0

# Event context
PRE_EVENT_SEC    = 5.0
POST_EVENT_SEC   = 5.0
MERGE_GAP_SEC    = 10.0

# Processing loop
SCAN_INTERVAL_SEC = 10.0

# Binary format (must match collector)
FILE_MAGIC    = b"ACLB"
FILE_VERSION  = 1
HEADER_FORMAT = "<4sBBBBHfd"
HEADER_SIZE   = struct.calcsize(HEADER_FORMAT)  # 22
RECORD_FORMAT = "<dfff"
RECORD_SIZE   = struct.calcsize(RECORD_FORMAT)  # 20


# --------- FILE PARSING ---------

def parse_raw_filename(path):
    """Parse bus and addr from filename like accel_bus0_0x19_1770773050.dat"""
    name = os.path.basename(path).replace(".dat", "")
    parts = name.split("_")
    # parts: [accel, bus0, 0x19, 1770773050]
    if len(parts) != 4 or not parts[1].startswith("bus"):
        return None, None, None
    try:
        bus = int(parts[1][3:])
        addr = int(parts[2], 16)
        ts = int(parts[3])
        return bus, addr, ts
    except (ValueError, IndexError):
        return None, None, None


def read_binary_file(path, log):
    """Read and validate a binary accel file.
    Returns (bus, addr, sample_rate, samples) or None on error.
    samples is a list of (timestamp, ax, ay, az) tuples.
    """
    try:
        file_size = os.path.getsize(path)
        if file_size < HEADER_SIZE:
            log.error("File too small (%d bytes): %s", file_size, path)
            return None

        with open(path, "rb") as f:
            raw_header = f.read(HEADER_SIZE)
            magic, ver, bus, addr, fs, rate, sens, start_ts = \
                struct.unpack(HEADER_FORMAT, raw_header)

            if magic != FILE_MAGIC:
                log.error("Bad magic in %s", path)
                return None
            if ver != FILE_VERSION:
                log.error("Unknown version %d in %s", ver, path)
                return None

            raw_data = f.read()

        n_records = len(raw_data) // RECORD_SIZE
        if n_records == 0:
            return None

        # Trim partial record at end
        raw_data = raw_data[:n_records * RECORD_SIZE]
        samples = list(struct.iter_unpack(RECORD_FORMAT, raw_data))

        return bus, addr, rate, samples

    except Exception as e:
        log.error("Failed to read %s: %s", path, e)
        return None


# --------- EVENT DETECTION ---------

def detect_events(samples, sample_rate):
    """Detect anomaly events in sample data.
    Returns list of (start_idx, end_idx, first_trigger_ts) tuples.
    Returns empty list if no events found.
    """
    n = len(samples)
    if n == 0:
        return []

    # Compute vector magnitudes
    magnitudes = [0.0] * n
    for i in range(n):
        ax = samples[i][1]
        ay = samples[i][2]
        az = samples[i][3]
        magnitudes[i] = math.sqrt(ax * ax + ay * ay + az * az)

    # Rolling RMS (causal, 2s window)
    window = max(1, int(RMS_WINDOW_SEC * sample_rate))
    rms = [0.0] * n

    # Bootstrap: compute initial RMS from first window
    init_count = min(window, n)
    sum_sq = 0.0
    for i in range(init_count):
        sum_sq += magnitudes[i] * magnitudes[i]
    initial_rms = math.sqrt(sum_sq / init_count)

    # Fill initial samples with bootstrap RMS
    for i in range(min(window, n)):
        rms[i] = initial_rms

    # Sliding window for the rest
    if n > window:
        sum_sq = 0.0
        for i in range(window):
            sum_sq += magnitudes[i] * magnitudes[i]
        for i in range(window, n):
            rms[i] = math.sqrt(sum_sq / window)
            sum_sq += magnitudes[i] * magnitudes[i]
            sum_sq -= magnitudes[i - window] * magnitudes[i - window]
            # Guard against floating point drift
            if sum_sq < 0.0:
                sum_sq = 0.0

    # Find trigger samples
    pre_samples = int(PRE_EVENT_SEC * sample_rate)
    post_samples = int(POST_EVENT_SEC * sample_rate)
    merge_samples = int(MERGE_GAP_SEC * sample_rate)

    triggers = []
    for i in range(n):
        mag = magnitudes[i]
        if mag > ABS_THRESHOLD_G or mag > REL_MULTIPLIER * rms[i]:
            triggers.append(i)

    if not triggers:
        return []

    # Create padded windows around triggers
    windows = []
    for t in triggers:
        start = max(0, t - pre_samples)
        end = min(n - 1, t + post_samples)
        trigger_ts = samples[t][0]
        windows.append((start, end, trigger_ts))

    # Merge overlapping or close windows
    merged = [windows[0]]
    for i in range(1, len(windows)):
        start, end, trigger_ts = windows[i]
        prev_start, prev_end, prev_trigger_ts = merged[-1]
        if start <= prev_end + merge_samples:
            # Merge: extend end, keep first trigger timestamp
            merged[-1] = (prev_start, max(prev_end, end), prev_trigger_ts)
        else:
            merged.append((start, end, trigger_ts))

    return merged


# --------- EVENT OUTPUT ---------

def write_event_csv(bus, addr, first_trigger_ts, samples, start_idx, end_idx, log):
    """Write one event to a CSV file with .tmp -> .dat atomic rename.
    Returns True on success.
    """
    ts_int = int(first_trigger_ts)
    base = "accel_bus%d_0x%02x_event_%d" % (bus, addr, ts_int)
    tmp_path = os.path.join(EVENT_DIR, base + ".dat.tmp")
    final_path = os.path.join(EVENT_DIR, base + ".dat")

    addr_str = "0x%02x" % addr

    try:
        with open(tmp_path, "w") as f:
            for i in range(start_idx, end_idx + 1):
                ts, ax, ay, az = samples[i]
                f.write("%.4f,%d,%s,%.3f,%.3f,%.3f\n" % (
                    ts, bus, addr_str, ax, ay, az))
            f.flush()
            os.fsync(f.fileno())

        os.rename(tmp_path, final_path)
        return True

    except Exception as e:
        log.error("Failed to write event %s: %s", base, e)
        # Clean up partial file
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass
        return False


# --------- MAIN PROCESSOR ---------

class EventProcessor:
    def __init__(self):
        self.running = True
        self.files_processed = 0
        self.events_written = 0

        self.log = logging.getLogger("process_accel")
        self.log.setLevel(logging.INFO)

        fh = logging.FileHandler(LOG_PATH)
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

    def _get_sealed_files(self):
        """List all sealed .dat files in RAW_DIR, sorted by name (timestamp order)."""
        try:
            files = []
            for name in os.listdir(RAW_DIR):
                if name.startswith("accel_") and name.endswith(".dat") \
                        and not name.endswith(".tmp"):
                    files.append(os.path.join(RAW_DIR, name))
            files.sort()
            return files
        except FileNotFoundError:
            return []
        except Exception as e:
            self.log.error("Failed to list %s: %s", RAW_DIR, e)
            return []

    def _process_file(self, path):
        """Process one binary accel file. Detect events, write CSVs, delete raw."""
        result = read_binary_file(path, self.log)
        if result is None:
            self.log.error("Skipping unreadable file: %s", path)
            self._safe_delete(path)
            return

        bus, addr, sample_rate, samples = result
        n_samples = len(samples)

        # Detect events
        events = detect_events(samples, sample_rate)

        if events:
            for start_idx, end_idx, trigger_ts in events:
                n_event = end_idx - start_idx + 1
                duration = samples[end_idx][0] - samples[start_idx][0]
                ok = write_event_csv(bus, addr, trigger_ts,
                                     samples, start_idx, end_idx, self.log)
                if ok:
                    self.events_written += 1
                    self.log.info(
                        "Event: bus%d 0x%02x t=%d, %d samples (%.1fs)",
                        bus, addr, int(trigger_ts), n_event, duration)

        # Done with this file
        self._safe_delete(path)
        self.files_processed += 1

    def _safe_delete(self, path):
        """Delete a processed raw file."""
        try:
            os.unlink(path)
        except Exception as e:
            self.log.error("Failed to delete %s: %s", path, e)

    def run(self):
        self.log.info("Starting event processor")
        self.log.info("Config: abs=%.1fg, rel=%.1fx, rms_win=%.1fs, "
                      "pre=%.1fs, post=%.1fs, merge=%.1fs",
                      ABS_THRESHOLD_G, REL_MULTIPLIER, RMS_WINDOW_SEC,
                      PRE_EVENT_SEC, POST_EVENT_SEC, MERGE_GAP_SEC)
        self.log.info("Input: %s, Output: %s", RAW_DIR, EVENT_DIR)

        last_status_time = time.time()

        while self.running:
            try:
                files = self._get_sealed_files()
                for path in files:
                    if not self.running:
                        break
                    self._process_file(path)

                # Status every 5 minutes
                now = time.time()
                if now - last_status_time >= 300:
                    self.log.info("OK: %d files processed, %d events written",
                                  self.files_processed, self.events_written)
                    last_status_time = now

            except Exception as e:
                self.log.error("Scan error: %s", e)
                self.log.error(traceback.format_exc())

            # Wait for next scan
            for _ in range(int(SCAN_INTERVAL_SEC * 10)):
                if not self.running:
                    break
                time.sleep(0.1)

        self.log.info("Stopped (%d files processed, %d events written)",
                      self.files_processed, self.events_written)


def main():
    processor = EventProcessor()
    try:
        processor.run()
    except Exception as e:
        processor.log.critical("FATAL: %s", e)
        processor.log.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
