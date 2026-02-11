#!/usr/bin/env python3
"""
AHT30 Temperature and Humidity Sensor Data Logger
Samples once per second and logs to /dev/shm/temp.dat
"""

import smbus2
from smbus2 import i2c_msg
import time
import sys
import traceback

# --------- CONFIGURATION ---------
I2C_BUS = 1
AHT30_ADDR = 0x38

SAMPLE_RATE_HZ = 1  # Sample once per second
OUTPUT_FILE = '/dev/shm/temp.dat'
FLUSH_INTERVAL = 10  # Flush file every N samples
FILE_REOPEN_INTERVAL = 300  # Reopen file every 5 minutes to allow external access

# Timing
MEASUREMENT_DELAY = 0.080  # 80ms per datasheet
POWER_ON_DELAY = 0.005     # 5ms after power on

# Error handling
MAX_CONSECUTIVE_ERRORS = 10
ERROR_COOLDOWN_SEC = 1.0
I2C_RESET_COOLDOWN_SEC = 5.0

# --------- UTILITY FUNCTIONS ---------

def log_message(msg):
    """Log message with timestamp to stdout"""
    from datetime import datetime
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {msg}", flush=True)


def timestamp_unix():
    """Return Unix timestamp with milliseconds"""
    return time.time()


def calc_crc8(data):
    """
    Calculate CRC8 checksum for AHT30
    Polynomial: x^8 + x^5 + x^4 + 1 (0x31)
    Initial value: 0xFF
    """
    crc = 0xFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = (crc << 1) ^ 0x31
            else:
                crc = crc << 1
            crc &= 0xFF  # Keep it 8-bit
    return crc


def read_aht30_temperature_humidity(bus):
    """
    Read temperature and humidity from AHT30 sensor
    Returns (temperature in Celsius, humidity in %RH)
    """
    # Send measurement command: 0xAC 0x33 0x00
    bus.write_i2c_block_data(AHT30_ADDR, 0xAC, [0x33, 0x00])
    
    # Wait for measurement to complete (80ms per datasheet)
    time.sleep(MEASUREMENT_DELAY)
    
    # Read 7 bytes using raw I2C read (no register address)
    msg = i2c_msg.read(AHT30_ADDR, 7)
    bus.i2c_rdwr(msg)
    data = list(msg)
    
    # Verify CRC
    calculated_crc = calc_crc8(data[0:6])
    if calculated_crc != data[6]:
        raise ValueError(f"CRC mismatch: calculated {calculated_crc:02X}, received {data[6]:02X}")
    
    # Check if sensor is busy (bit 7 of status byte)
    if data[0] & 0x80:
        raise ValueError("Sensor busy flag set")
    
    # Extract 20-bit humidity value
    # Humidity is in bytes 1-3: SRH[19:12] in byte 1, SRH[11:4] in byte 2, SRH[3:0] in upper 4 bits of byte 3
    humidity_raw = (data[1] << 12) | (data[2] << 4) | (data[3] >> 4)
    
    # Extract 20-bit temperature value
    # Temperature is in bytes 3-5: ST[19:16] in lower 4 bits of byte 3, ST[15:8] in byte 4, ST[7:0] in byte 5
    temp_raw = ((data[3] & 0x0F) << 16) | (data[4] << 8) | data[5]
    
    # Convert to Celsius and %RH using formulas from datasheet
    # Temperature = (ST / 2^20) * 200 - 50
    # Humidity = (SRH / 2^20) * 100
    temperature = (temp_raw / 1048576.0) * 200.0 - 50.0
    humidity = (humidity_raw / 1048576.0) * 100.0
    
    return temperature, humidity


def init_aht30(bus):
    """Initialize AHT30 sensor"""
    try:
        # Give sensor time to power up
        time.sleep(POWER_ON_DELAY)
        
        # Try to read status to verify sensor is present
        status = bus.read_byte_data(AHT30_ADDR, 0x00)
        
        log_message(f"AHT30 initialized successfully (status: 0x{status:02X})")
        return True
    except Exception as e:
        log_message(f"ERROR initializing AHT30: {e}")
        return False


# --------- MAIN LOGGER CLASS ---------

class TempLogger:
    def __init__(self):
        self.bus = None
        self.output_file = None
        self.sample_count = 0
        self.consecutive_errors = 0
        self.last_file_reopen = time.time()
        self.last_successful_read = None
        
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
                if init_aht30(self.bus):
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
    
    def write_sample(self, timestamp, temperature, humidity):
        """Write a single sample to output file"""
        try:
            # Format: timestamp,temperature,humidity
            line = f"{timestamp:.3f},{temperature:.2f},{humidity:.2f}\n"
            self.output_file.write(line)
            
            # Flush periodically to ensure data is written
            if self.sample_count % FLUSH_INTERVAL == 0:
                self.output_file.flush()
            
            return True
        except Exception as e:
            log_message(f"ERROR writing sample: {e}")
            return False
    
    def read_and_log_sample(self):
        """Read temperature sensor and write sample to file"""
        try:
            # Read sensor data
            temperature, humidity = read_aht30_temperature_humidity(self.bus)
            
            # Get timestamp
            ts = timestamp_unix()
            
            # Write to file
            if self.write_sample(ts, temperature, humidity):
                self.sample_count += 1
                self.consecutive_errors = 0
                self.last_successful_read = (temperature, humidity)
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
        log_message("Temperature Logger starting...")
        
        # Initial setup
        if not self.open_bus():
            log_message("FATAL: Could not initialize I2C bus")
            sys.exit(1)
        
        if not self.open_output_file():
            log_message("FATAL: Could not open output file")
            sys.exit(1)
        
        log_message(f"Logging started at {SAMPLE_RATE_HZ}Hz (once per second) to {OUTPUT_FILE}")
        
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
                # Note: AHT30 measurement takes ~80ms, well within 1 second interval
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
    logger = TempLogger()
    try:
        logger.run()
    except Exception as e:
        log_message(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

