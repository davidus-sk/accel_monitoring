#!/usr/bin/python3

import socket
import curses
import math
import sys

# Configuration
TCP_IP = '127.0.0.1'
BUFFER_SIZE = 1024
SCALE = 20

# State storage: tracks data for every sensor ID encountered
# Format: { '0x19': {'X': 0.0, 'Y': 0.0, 'Z': 0.0, 'MaxX': 0.0, ...}, '0x20': {...} }
sensors = {}

def update_sensor_data(parts):
    """Parses CSV parts and updates the sensor's state."""
    try:
        ts = parts[0]
        bus = parts[1]
        sid = parts[2]
        ax, ay, az = float(parts[3]), float(parts[4]), float(parts[5])
        mag = math.sqrt(ax**2 + ay**2 + az**2)

        if sid not in sensors:
            sensors[sid] = {
                'X': 0, 'Y': 0, 'Z': 0, 'Mag': 0,
                'max_X': 0, 'max_Y': 0, 'max_Z': 0, 'max_Mag': 0,
                'ts': ts, 'bus': bus
            }

        s = sensors[sid]
        s['X'], s['Y'], s['Z'], s['Mag'] = ax, ay, az, mag
        s['ts'], s['bus'] = ts, bus

        # Update maximums
        s['max_X'] = max(s['max_X'], abs(ax))
        s['max_Y'] = max(s['max_Y'], abs(ay))
        s['max_Z'] = max(s['max_Z'], abs(az))
        s['max_Mag'] = max(s['max_Mag'], abs(mag))

    except (ValueError, IndexError):
        pass

def draw_sensor(stdscr, sid, start_row):
    """Draws the visualization block for a single sensor."""
    s = sensors[sid]

    # Header
    stdscr.addstr(start_row, 2, f"SENSOR ID: {sid} | Bus: {s['bus']} | TS: {s['ts']}", curses.A_BOLD | curses.A_UNDERLINE)

    # Data Rows
    metrics = [
        ("Acc X", s['X'], s['max_X'], 1),
        ("Acc Y", s['Y'], s['max_Y'], 1),
        ("Acc Z", s['Z'], s['max_Z'], 1),
        ("MAGNITUDE", s['Mag'], s['max_Mag'], 2)
    ]

    current_row = start_row + 1
    for label, val, m_val, color_pair in metrics:
        if label == "MAGNITUDE":
            current_row += 1 # Add spacing before magnitude

        display_text = f"{label:10}: {val:>7.3f} g [Max: {m_val:>7.3f} g] | "
        bar_len = min(int(abs(val) * SCALE), curses.COLS - len(display_text) - 5)
        bar = "█" * max(0, bar_len)

        stdscr.addstr(current_row, 2, display_text)
        stdscr.addstr(current_row, len(display_text) + 2, bar, curses.color_pair(color_pair))
        current_row += 1

    return current_row + 1 # Return next available row position

def main(stdscr, port):
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.curs_set(0)
    stdscr.nodelay(True)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((TCP_IP, port))
    except Exception as e:
        return f"Connection Error: {e}"

    data_buffer = ""
    while True:
        key = stdscr.getch()
        if key == ord('q'): break
        if key == ord('r'):
            for sid in sensors:
                for k in sensors[sid]:
                    if k.startswith('max_'): sensors[sid][k] = 0

        try:
            chunk = s.recv(BUFFER_SIZE).decode('utf-8')
            if not chunk: break
            data_buffer += chunk

            if "\n" in data_buffer:
                lines = data_buffer.split("\n")
                data_buffer = lines.pop()

                for line in lines:
                    update_sensor_data(line.strip().split(','))

                # UI Update
                stdscr.erase()
                stdscr.addstr(0, 2, f"MULTI-IMU MONITOR | Port: {port} | 'q' Quit | 'r' Reset Max", curses.A_BOLD)

                row = 2
                # Sort IDs so the display doesn't jump around
                for sid in sorted(sensors.keys()):
                    row = draw_sensor(stdscr, sid, row)

                stdscr.refresh()
        except Exception:
            continue
    s.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python imu_vis.py <port>")
        sys.exit(1)
    err = curses.wrapper(main, int(sys.argv[1]))
    if err: print(err)
