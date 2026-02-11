#!/usr/bin/env python3
"""
MPU-6050 IMU Data Logger
Samples at 40Hz and logs to /dev/shm/imu.dat
"""

import smbus2
import time
import math
import sys
import traceback
from datetime import datetime

# --------- CONFIGURATION ---------
I2C_BUS = 1
MPU6050_ADDR = 0x68
PWR_MGMT_1 = 0x6B
ACCEL_XOUT_H = 0x3B
GYRO_XOUT_H = 0x43
ACCEL_CONFIG = 0x1C
GYRO_CONFIG = 0x1B

# Scale factors for ±8g accel and ±250°/s gyro
ACCEL_SCALE = 4096.0   # ±8g range
GYRO_SCALE = 131.0     # ±250°/s range

SAMPLE_RATE_HZ = 40
OUTPUT_FILE = '/dev/shm/imu.dat'
FLUSH_INTERVAL = 10  # Flush file every N samples
FILE_REOPEN_INTERVAL = 300  # Reopen file every 5 minutes to allow external access

# Error handling
MAX_CONSECUTIVE_ERRORS = 10
ERROR_COOLDOWN_SEC = 1.0
I2C_RESET_COOLDOWN_SEC = 5.0

# --------- UTILITY FUNCTIONS ---------

def read_word(bus, addr, reg):
    """Read a 16-bit signed value from two consecutive registers"""
    try:
        high = bus.read_byte_data(addr, reg)
        low = bus.read_byte_data(addr, reg + 1)
        value = (high << 8) + low
        return value if value < 0x8000 else value - 65536
    except OSError as e:
        # I2C communication error
        raise IOError(f"I2C read error at register 0x{reg:02X}: {e}")


def calibrate_gyro(bus, num_samples=100):
    """
    Calibrate gyro by measuring bias when stationary
    Returns: (gx_offset, gy_offset, gz_offset)
    """
    print(f"[{timestamp_ms()}] Calibrating gyro (keep sensor stationary)...", flush=True)
    
    gx_sum, gy_sum, gz_sum = 0.0, 0.0, 0.0
    successful_samples = 0
    
    for i in range(num_samples):
        try:
            data = bus.read_i2c_block_data(MPU6050_ADDR, ACCEL_XOUT_H, 14)
            
            # Parse gyroscope data (bytes 8-13)
            gx_raw = (data[8] << 8) | data[9]
            gy_raw = (data[10] << 8) | data[11]
            gz_raw = (data[12] << 8) | data[13]
            
            # Convert to signed and to °/s
            gx = (gx_raw if gx_raw < 0x8000 else gx_raw - 65536) / GYRO_SCALE
            gy = (gy_raw if gy_raw < 0x8000 else gy_raw - 65536) / GYRO_SCALE
            gz = (gz_raw if gz_raw < 0x8000 else gz_raw - 65536) / GYRO_SCALE
            
            gx_sum += gx
            gy_sum += gy
            gz_sum += gz
            successful_samples += 1
            
            time.sleep(0.01)  # 10ms between samples
        except:
            continue
    
    if successful_samples < 50:
        print(f"[{timestamp_ms()}] WARNING: Gyro calibration failed, using no offset", flush=True)
        return 0.0, 0.0, 0.0
    
    gx_offset = gx_sum / successful_samples
    gy_offset = gy_sum / successful_samples
    gz_offset = gz_sum / successful_samples
    
    print(f"[{timestamp_ms()}] Gyro offsets: X={gx_offset:.3f}, Y={gy_offset:.3f}, Z={gz_offset:.3f} °/s", flush=True)
    return gx_offset, gy_offset, gz_offset


def get_mpu6050(bus, gyro_offsets=(0.0, 0.0, 0.0)):
    """Read accelerometer and gyroscope data from MPU-6050 using block read"""
    try:
        # Read all 14 bytes at once (more efficient than individual reads)
        # Accel: 6 bytes, Temp: 2 bytes, Gyro: 6 bytes
        data = bus.read_i2c_block_data(MPU6050_ADDR, ACCEL_XOUT_H, 14)
        
        # Parse accelerometer data (bytes 0-5)
        ax_raw = (data[0] << 8) | data[1]
        ay_raw = (data[2] << 8) | data[3]
        az_raw = (data[4] << 8) | data[5]
        
        # Parse gyroscope data (bytes 8-13, skip temp at 6-7)
        gx_raw = (data[8] << 8) | data[9]
        gy_raw = (data[10] << 8) | data[11]
        gz_raw = (data[12] << 8) | data[13]
        
        # Convert to signed values
        def to_signed(val):
            return val if val < 0x8000 else val - 65536
        
        ax = to_signed(ax_raw) / ACCEL_SCALE
        ay = to_signed(ay_raw) / ACCEL_SCALE
        az = to_signed(az_raw) / ACCEL_SCALE
        gx = to_signed(gx_raw) / GYRO_SCALE - gyro_offsets[0]
        gy = to_signed(gy_raw) / GYRO_SCALE - gyro_offsets[1]
        gz = to_signed(gz_raw) / GYRO_SCALE - gyro_offsets[2]
        
        return ax, ay, az, gx, gy, gz
    except OSError as e:
        raise IOError(f"I2C block read error: {e}")


def timestamp_ms():
    """Return human-readable timestamp for logging"""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def timestamp_unix():
    """Return Unix timestamp with milliseconds: seconds.milliseconds"""
    return time.time()


def init_mpu6050(bus):
    """Initialize MPU-6050 sensor with proper configuration"""
    try:
        # Wake up the MPU-6050 (it starts in sleep mode)
        bus.write_byte_data(MPU6050_ADDR, PWR_MGMT_1, 0)
        time.sleep(0.1)
        
        # Configure accelerometer range to ±8g (bits 4:3 = 10, value 0x10)
        bus.write_byte_data(MPU6050_ADDR, ACCEL_CONFIG, 0x10)
        time.sleep(0.01)
        
        # Configure gyroscope range to ±250°/s (bits 4:3 = 00)
        bus.write_byte_data(MPU6050_ADDR, GYRO_CONFIG, 0x00)
        time.sleep(0.1)
        
        # Do a test read to ensure it's ready and verify configuration
        try:
            data = bus.read_i2c_block_data(MPU6050_ADDR, ACCEL_XOUT_H, 14)
            
            # Verify configuration was set correctly
            accel_cfg = bus.read_byte_data(MPU6050_ADDR, ACCEL_CONFIG)
            gyro_cfg = bus.read_byte_data(MPU6050_ADDR, GYRO_CONFIG)
            
            print(f"[{timestamp_ms()}] MPU-6050 initialized successfully", flush=True)
            print(f"[{timestamp_ms()}] Accel config: 0x{accel_cfg:02X} (±8g), Gyro config: 0x{gyro_cfg:02X} (±250°/s)", flush=True)
            return True
        except:
            time.sleep(0.5)
            # Try one more time
            data = bus.read_i2c_block_data(MPU6050_ADDR, ACCEL_XOUT_H, 14)
            print(f"[{timestamp_ms()}] MPU-6050 initialized successfully (retry)", flush=True)
            return True
        
    except Exception as e:
        print(f"[{timestamp_ms()}] ERROR initializing MPU-6050: {e}", flush=True)
        return False


def log_message(msg):
    """Log message with timestamp to stdout"""
    print(f"[{timestamp_ms()}] {msg}", flush=True)


# --------- MAIN LOGGER CLASS ---------

class IMULogger:
    def __init__(self):
        self.bus = None
        self.output_file = None
        self.sample_count = 0
        self.consecutive_errors = 0
        self.last_file_reopen = time.time()
        self.gyro_offsets = (0.0, 0.0, 0.0)  # Gyro calibration offsets
        
    def open_bus(self):
        """Open I2C bus with error handling"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                if self.bus:
                    try:
                        self.bus.close()
                    except:
                        pass
                
                self.bus = smbus2.SMBus(I2C_BUS)
                if init_mpu6050(self.bus):
                    # Calibrate gyro to remove bias
                    self.gyro_offsets = calibrate_gyro(self.bus)
                    self.consecutive_errors = 0
                    return True
                else:
                    time.sleep(ERROR_COOLDOWN_SEC)
            except Exception as e:
                log_message(f"Bus open attempt {attempt+1}/{max_retries} failed: {e}")
                time.sleep(ERROR_COOLDOWN_SEC * (attempt + 1))
        
        return False
    
    def open_output_file(self):
        """Open output file in append mode"""
        try:
            if self.output_file:
                try:
                    self.output_file.close()
                except:
                    pass
            
            self.output_file = open(OUTPUT_FILE, 'a', buffering=1)  # Line buffered
            self.last_file_reopen = time.time()
            log_message(f"Output file opened: {OUTPUT_FILE}")
            return True
        except Exception as e:
            log_message(f"ERROR opening output file: {e}")
            return False
    
    def reopen_file_if_needed(self):
        """Periodically reopen file to allow external processes to access it"""
        if time.time() - self.last_file_reopen > FILE_REOPEN_INTERVAL:
            log_message("Reopening output file for external access")
            return self.open_output_file()
        return True
    
    def write_sample(self, timestamp, ax, ay, az, gx, gy, gz):
        """Write a single sample to output file"""
        try:
            # Format: timestamp,ax,ay,az,gx,gy,gz
            # Timestamp is Unix time with milliseconds (e.g., 1763030694.112)
            line = f"{timestamp:.3f},{ax:.6f},{ay:.6f},{az:.6f},{gx:.3f},{gy:.3f},{gz:.3f}\n"
            self.output_file.write(line)
            
            # Flush periodically to ensure data is written
            if self.sample_count % FLUSH_INTERVAL == 0:
                self.output_file.flush()
            
            return True
        except Exception as e:
            log_message(f"ERROR writing sample: {e}")
            return False
    
    def read_and_log_sample(self):
        """Read IMU and write sample to file"""
        try:
            # Read sensor data with gyro offset correction
            ax, ay, az, gx, gy, gz = get_mpu6050(self.bus, self.gyro_offsets)
            
            # Get Unix timestamp with microseconds
            ts = timestamp_unix()
            
            # Write to file
            if self.write_sample(ts, ax, ay, az, gx, gy, gz):
                self.sample_count += 1
                
                # Log recovery message
                if self.consecutive_errors > 0:
                    log_message(f"Sensor read recovered after {self.consecutive_errors} errors")
                self.consecutive_errors = 0
                
                # Small delay after first read to help stabilize timing
                if self.sample_count == 1:
                    time.sleep(0.01)
                
                return True
            else:
                self.consecutive_errors += 1
                return False
                
        except Exception as e:
            self.consecutive_errors += 1
            if self.consecutive_errors <= 3 or self.consecutive_errors % 10 == 0:
                log_message(f"ERROR reading sensor (count={self.consecutive_errors}): {e}")
            return False
    
    def run(self):
        """Main logging loop with robust error handling"""
        log_message("IMU Logger starting...")
        
        # Initial setup
        if not self.open_bus():
            log_message("FATAL: Could not initialize I2C bus")
            sys.exit(1)
        
        if not self.open_output_file():
            log_message("FATAL: Could not open output file")
            sys.exit(1)
        
        log_message(f"Logging started at {SAMPLE_RATE_HZ}Hz to {OUTPUT_FILE}")
        
        sample_interval = 1.0 / SAMPLE_RATE_HZ
        next_sample_time = time.time()
        
        while True:
            try:
                # Check if too many consecutive errors - reinitialize
                if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log_message(f"Too many errors ({self.consecutive_errors}), reinitializing...")
                    time.sleep(I2C_RESET_COOLDOWN_SEC)
                    
                    if not self.open_bus():
                        log_message("Bus reinitialization failed, retrying...")
                        time.sleep(ERROR_COOLDOWN_SEC)
                        continue
                    
                    if not self.open_output_file():
                        log_message("File reopen failed, retrying...")
                        time.sleep(ERROR_COOLDOWN_SEC)
                        continue
                    
                    self.consecutive_errors = 0
                    log_message("Reinitialization successful")
                
                # Periodically reopen file for external access
                if not self.reopen_file_if_needed():
                    log_message("File reopen failed, continuing with current handle")
                
                # Read and log sample
                self.read_and_log_sample()
                
                # Timing: sleep until next sample time
                next_sample_time += sample_interval
                sleep_time = next_sample_time - time.time()
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    # We're running behind schedule
                    if sleep_time < -0.1:  # Only log if significantly behind
                        log_message(f"Warning: Running {-sleep_time:.3f}s behind schedule")
                    next_sample_time = time.time()  # Reset timing
                
            except KeyboardInterrupt:
                log_message("Received keyboard interrupt, shutting down...")
                break
            except Exception as e:
                log_message(f"Unexpected error in main loop: {e}")
                traceback.print_exc()
                self.consecutive_errors += 1
                time.sleep(ERROR_COOLDOWN_SEC)
        
        # Cleanup
        try:
            if self.output_file:
                self.output_file.flush()
                self.output_file.close()
            if self.bus:
                self.bus.close()
            log_message("Shutdown complete")
        except Exception as e:
            log_message(f"Error during cleanup: {e}")


# --------- MAIN ENTRY POINT ---------

def main():
    logger = IMULogger()
    try:
        logger.run()
    except Exception as e:
        log_message(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

