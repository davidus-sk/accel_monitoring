#!/usr/bin/env python3

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import time
import glob
import smtplib
import re
import subprocess
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

def get_modem_list():
    """Returns a list of modem indices found on the system."""
    try:
        result = subprocess.check_output(["mmcli", "-L", "-J"], stderr=subprocess.STDOUT)
        data = json.loads(result)
        # Extract indices from the 'modem-list' array
        return [m.split('/')[-1] for m in data.get("modem-list", [])]
    except Exception as e:
        print(f"Error listing modems: {e}")
        return []

def get_modem_imei(modem_index=0):
    """
    Retrieves the IMEI of a modem using mmcli.

    Args:
        modem_index (int/str): The index of the modem (default is 0).

    Returns:
        str: The 15-digit IMEI if found, else None.
    """
    try:
        # Execute mmcli command for the specific modem
        cmd = ["mmcli", "-m", str(modem_index)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Search for the IMEI line in the output
        # mmcli output usually looks like: "equipment | imei: '123456789012345'"
        match = re.search(r"imei:\s+'?(\d+)'?", result.stdout)

        if match:
            return match.group(1)
        else:
            print(f"IMEI not found in mmcli output for modem {modem_index}.")
            return None

    except subprocess.CalledProcessError as e:
        print(f"Error calling mmcli: {e.stderr}")
        return None
    except FileNotFoundError:
        print("mmcli is not installed on this system.")
        return None

def strip_html_tags_regex(html_string: str) -> str:
    """
    Strips HTML tags from a string using a simple regular expression.

    NOTE: This method is fast but is not robust for complex, nested, 
    or malformed HTML. For production code, consider the HTMLParser method.

    :param html_string: The input string potentially containing HTML tags.
    :return: The string with HTML tags removed.
    """
    # Regex to find anything enclosed in < and >
    clean = re.compile('<.*?>')
    return re.sub(clean, '', html_string)

def send_outlook_email(sender_email, sender_password, recipient_email, subject, body_text, body_html=None):
    """
    Sends an email using Outlook.com / Office 365 SMTP servers.

    Args:
        sender_email (str): Your Outlook/Hotmail/Live email address.
        sender_password (str): Your App Password (recommended) or login password.
        recipient_email (str): The email address of the receiver.
        subject (str): The subject line of the email.
        body_text (str): The plain text body of the email.
        body_html (str, optional): The HTML body of the email. Defaults to None.
    """

    # Outlook SMTP server settings
    smtp_server = "smtp.office365.com"
    smtp_port = 587

    # Create the email object
    msg = MIMEMultipart('alternative')
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Attach the body text (always required as fallback)
    msg.attach(MIMEText(body_text, 'plain'))

    # Attach the HTML body if provided
    if body_html:
        msg.attach(MIMEText(body_html, 'html'))

    try:
        # Connect to the server
        print(f"Connecting to {smtp_server}...")
        server = smtplib.SMTP(smtp_server, smtp_port)

        # Secure the connection
        server.starttls()

        # Login
        print("Logging in...")
        server.login(sender_email, sender_password)

        # Send the email
        print(f"Sending email to {recipient_email}...")
        server.send_message(msg)

        # Disconnect
        server.quit()
        print("Email sent successfully!")
        return True

    except smtplib.SMTPAuthenticationError:
        print("\nERROR: Authentication failed.")
        print("If you have 2FA enabled, you MUST use an 'App Password'.")
        print("Check your Microsoft Account -> Security -> Advanced Security Options.")
        return False
    except Exception as e:
        print(f"\nERROR: An error occurred: {e}")
        return False

def group_and_filter_impacts():
    # 1. Generate the filename for yesterday
    yesterday = datetime.now() - timedelta(days=1)
    file_name = yesterday.strftime('/tmp/data_%Y-%m-%d.log')

    print(f"Processing: {file_name}")

    column_names = ['timestamp', 'bus_id', 'sensor_id', 'magnitude', 'median', 'deflection']
    try:
        df = pd.read_csv(file_name, names=column_names)
    except FileNotFoundError:
        print(f"File {file_name} not found.")
        return []

    if df.empty:
        return []

    # 2. Prepare Timestamps
    df['dt'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.sort_values('dt')

    # 3. Group by 2-minute windows and pick the max magnitude
    reduced_df = (
        df.groupby(pd.Grouper(key='dt', freq='250s'))
        .apply(lambda x: x.loc[x['magnitude'].idxmax()] if not x.empty else None)
        .dropna()
        .reset_index(drop=True)
    )

    # 4. Filter: Keep only events where magnitude > x
    filtered_df = reduced_df[reduced_df['magnitude'] > 20].copy()

    # 5. Convert to final array for further processing
    # We drop the helper 'dt' column to keep the array clean
    impact_array = filtered_df.drop(columns=['dt']).to_dict(orient='records')

    # 6. Find corresponding impact
    #
    if impact_array:
        for i, event in enumerate(impact_array):
            other_sensor_id = '0x19' if event['sensor_id'] == '0x18' else '0x18'
            target_pool = df[df['sensor_id'] == other_sensor_id].sort_values('timestamp')
            idx = target_pool['timestamp'].searchsorted(event['timestamp'])

            if idx == 0:
                closest_row = target_pool.iloc[0]
            elif idx >= len(target_pool):
                closest_row = target_pool.iloc[-1]
            else:
                before = target_pool.iloc[idx - 1]
                after = target_pool.iloc[idx]

                if abs(after['timestamp'] - event['timestamp']) < abs(before['timestamp'] - event['timestamp']):
                    closest_row = after
                else:
                    closest_row = before

            impact_array[i]['other_sensor_id'] = closest_row['sensor_id']
            impact_array[i]['other_magnitude'] = closest_row['magnitude'].item()

    # return results
    return impact_array

# --- Execution ---
if __name__ == "__main__":
    high_impact_events = group_and_filter_impacts()

    if high_impact_events:
        print(f"Found {len(high_impact_events)} events with magnitude > 7.")

        # Email the results
        body = """
        <html>
        <body>
          <table cellspacing="0" cellpadding="5" border="5" width="600" style="width: 600px; border-collapse: collapse; border: 5px solid #cccccc;">
            <tr><td colspan="6" style="background-color: #0b4f8a; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: center; border: 5px solid #cccccc;"><b>Impact Report</b> <small>(sustained for at least 1.3ms)</small></td></tr>
            <tr>
              <th width="20%" style="width: 20%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: left; border: 5px solid #cccccc;">Date&nbsp;and&nbsp;Time</th>
              <th width="16%" style="width: 16%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: right; border: 5px solid #cccccc;">Sensor</th>
              <th width="16%" style="width: 16%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: right; border: 5px solid #cccccc;">Max</th>
              <th width="16%" style="width: 16%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: right; border: 5px solid #cccccc;">Other</th>
              <th width="16%" style="width: 16%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: right; border: 5px solid #cccccc;">Mean</th>
              <th width="16%" style="width: 16%; background-color: #337ab7; color: #ffffff; padding: 10px; font-family: Arial, sans-serif; font-size: 16px; text-align: right; border: 5px solid #cccccc;">Deflection</th>
            </tr>
        """

        total_events = 0

        for event in high_impact_events:
            string_time = datetime.fromtimestamp(event['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

            print(f"Time: {event['timestamp']} | Bus: {event['bus_id']} | Mag: {event['magnitude']:.2f}")

            body += f"""
            <tr>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;">{string_time}</td>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;text-align: right;">{event['sensor_id']}</td>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;text-align: right;">{event['magnitude']}g</td>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;text-align: right;">{event['other_magnitude']}g</td>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;text-align: right;">{event['median']}g</td>
              <td style="padding: 10px; font-family: Arial, sans-serif; font-size: 14px; color: #333333; border: 5px solid #cccccc;text-align: right;">{event['deflection']}</td>
            </tr>
            """

            total_events += 1

        string_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        yesterday = datetime.now() - timedelta(days=1)
        string_date = yesterday.strftime('%Y%m%d')

        modem_list = get_modem_list()
        if modem_list:
            imei = get_modem_imei(modem_list[0])
        else:
            imei = None

        body += f"""
            <tr><td colspan="6" style="background-color: #eeeeee; color: #000000; padding: 10px; font-family: Arial, sans-serif; font-size: 12px; text-align: center; border: 5px solid #cccccc;">&copy; 2025 LUCEON LLC. Generated on {string_time}. Event count: {total_events}.</td></tr>
          </table>
          <p>CSV download: <a href="https://daq.luceon.us/wabtec/csv.php?id={imei}_{string_date}_accel">https://daq.luceon.us/wabtec/csv.php?id={imei}_{string_date}_accel</a></p>
        </body>
        </html>
        """

        # Send email out
        for i in range(5):
            yesterday = datetime.now() - timedelta(days=1)
            string_time = yesterday.strftime("%Y-%m-%d")
            sent = send_outlook_email("x", "y", "z", f"Impact Report: {string_time}", strip_html_tags_regex(body), body)

            if sent:
                break

            time.sleep(10*i)

    else:
        print("No high-impact events (> 7) found for yesterday.")
