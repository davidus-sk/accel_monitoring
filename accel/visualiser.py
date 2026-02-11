#!/usr/bin/python3

import socket
import curses
import math

# Configuration
TCP_IP = '127.0.0.1'
TCP_PORT = 6000
BUFFER_SIZE = 1024
SCALE = 25 

# Dictionary to track the maximum absolute value for each component and the magnitude
max_values = {"X": 0.0, "Y": 0.0, "Z": 0.0, "Mag": 0.0}

def draw_row(stdscr, row, label, current_val, key):
    # Update global max
    if abs(current_val) > max_values[key]:
        max_values[key] = abs(current_val)

    # Format the text display
    display_text = f"{label:10}: {current_val:>7.3f} g  [Max: {max_values[key]:>7.3f} g] | "

    # Calculate bar length
    bar_len = int(abs(current_val) * SCALE)
    max_x = curses.COLS - len(display_text) - 5
    bar_len = min(bar_len, max_x)

    # Use different characters for component bars vs Magnitude
    char = "█" if key != "Mag" else "▓"
    bar = char * bar_len

    # Render to screen
    stdscr.addstr(row, 2, display_text)
    stdscr.addstr(row, len(display_text) + 2, bar)
    stdscr.clrtoeol()

def main(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.clear()

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((TCP_IP, TCP_PORT))
    except Exception as e:
        print(f"Error: {e}")
        return

    stdscr.addstr(1, 2, f"IMU MONITOR | Port: {TCP_PORT} | 'r' to reset | 'q' to quit")
    stdscr.addstr(2, 2, "=" * (curses.COLS - 4))

    data_buffer = ""

    while True:
        key_input = stdscr.getch()
        if key_input == ord('q'):
            break
        elif key_input == ord('r'):
            for k in max_values: max_values[k] = 0.0

        try:
            chunk = s.recv(BUFFER_SIZE).decode('utf-8')
            if not chunk: break

            data_buffer += chunk
            if "\n" in data_buffer:
                lines = data_buffer.split("\n")
                data_buffer = lines.pop()

                for line in lines:
                    parts = line.strip().split(',')
                    if len(parts) == 6:
                        try:
                            # Parse acceleration components
                            ax, ay, az = float(parts[3]), float(parts[4]), float(parts[5])

                            # Calculate Magnitude: sqrt(x^2 + y^2 + z^2)
                            mag = math.sqrt(ax**2 + ay**2 + az**2)

                            # Draw individual components
                            draw_row(stdscr, 4, "Acc X", ax, "X")
                            draw_row(stdscr, 5, "Acc Y", ay, "Y")
                            draw_row(stdscr, 6, "Acc Z", az, "Z")

                            # Draw Magnitude row
                            stdscr.addstr(7, 2, "-" * (curses.COLS - 4))
                            draw_row(stdscr, 8, "MAGNITUDE", mag, "Mag")
                            stdscr.addstr(10, 2, f"Latest Timestamp: {parts[0]}")
                            stdscr.refresh()
                        except ValueError:
                            continue

        except Exception:
            continue

    s.close()

if __name__ == "__main__":
    curses.wrapper(main)
