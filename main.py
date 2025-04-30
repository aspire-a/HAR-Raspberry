import asyncio
from bleak import BleakClient, BleakScanner
import json
import csv
from datetime import datetime
from flask import Flask, request, jsonify
import logging
from threading import Thread

# UUIDs for BLE service and characteristic
SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-123456789abc"

# Flask app initialization
app = Flask(__name__)

# Global state to store data from ESPs and activity
global_data = {
    "esp1": None,
    "esp2": None,
    "esp3": None,
    "esp4": None,
    "esp5": None
}
activity_data = None                           # Global variable to store activity data
data_lock     = asyncio.Lock()                 # For thread-safe access to global_data
connect_lock  = asyncio.Lock()                 # NEW: ensure one connect() at a time


# ---------------------------------------------------------------------
#  CSV helpers
# ---------------------------------------------------------------------
def initialize_csv_files():
    for i in range(1, 6):
        filename = f"esp{i}.csv"
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            headers = [
                "mpu1_ax", "mpu1_ay", "mpu1_az", "mpu1_gx", "mpu1_gy", "mpu1_gz",
                "mpu2_ax", "mpu2_ay", "mpu2_az", "mpu2_gx", "mpu2_gy", "mpu2_gz",
                "HMC_x", "HMC_y", "HMC_z", "Heading_degrees", "Date", "Time"
            ]
            writer.writerow(headers)

    # Create activity.csv file
    with open("activity.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        headers = [
            "activity_label", "activity_start_date", "activity_start_time",
            "activity_end_date", "activity_end_time"
        ]
        writer.writerow(headers)


def append_to_csv(device_index, data):
    filename = f"esp{device_index}.csv"
    with open(filename, mode="a", newline="") as file:
        csv.writer(file).writerow(data)


def append_activity_to_csv(activity):
    with open("activity.csv", mode="a", newline="") as file:
        csv.writer(file).writerow([
            activity["activity_label"],
            activity["activity_start_date"],
            activity["activity_start_time"],
            activity["activity_end_date"],
            activity["activity_end_time"]
        ])


# ---------------------------------------------------------------------
#  BLE handling
# ---------------------------------------------------------------------
async def update_global_data(key, value):
    async with data_lock:
        global_data[key] = value


async def connect_and_listen(device_name, ble_device, device_index):
    """Keep one connection to `ble_device` alive forever."""
    while True:
        client = None
        try:
            client = BleakClient(ble_device)

            # ---- CONNECT (serialized) ---------------------------------
            async with connect_lock:           # one connect / scan at a time
                await client.connect(timeout=8.0)
            # -----------------------------------------------------------

            print(f"Connected to {device_name} at {ble_device.address}")

            # -----------------------------------------------------------
            #  Notification callback
            # -----------------------------------------------------------
            def notification_handler(sender, data):
                try:
                    decoded_data = data.decode(errors="ignore")
                    sensor_data  = json.loads(decoded_data)

                    now      = datetime.now()
                    date_str = now.strftime("%Y-%m-%d")
                    time_str = now.strftime("%H:%M:%S")

                    row = [
                        sensor_data.get("mpu1", {}).get("ax", "N/A"),
                        sensor_data.get("mpu1", {}).get("ay", "N/A"),
                        sensor_data.get("mpu1", {}).get("az", "N/A"),
                        sensor_data.get("mpu1", {}).get("gx", "N/A"),
                        sensor_data.get("mpu1", {}).get("gy", "N/A"),
                        sensor_data.get("mpu1", {}).get("gz", "N/A"),
                        sensor_data.get("mpu2", {}).get("ax", "N/A"),
                        sensor_data.get("mpu2", {}).get("ay", "N/A"),
                        sensor_data.get("mpu2", {}).get("az", "N/A"),
                        sensor_data.get("mpu2", {}).get("gx", "N/A"),
                        sensor_data.get("mpu2", {}).get("gy", "N/A"),
                        sensor_data.get("mpu2", {}).get("gz", "N/A"),
                        sensor_data.get("HMCx", "N/A"),
                        sensor_data.get("HMCy", "N/A"),
                        sensor_data.get("HMCz", "N/A"),
                        sensor_data.get("Heading", "N/A"),
                        date_str,
                        time_str
                    ]

                    asyncio.create_task(update_global_data(f"esp{device_index}", row))
                    append_to_csv(device_index, row)
                except Exception as exc:
                    print(f"{device_name} – error processing data: {exc}")

            # start notifications
            await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
            print(f"Listening to {device_name} …")

            # Keep task alive while connected
            while client.is_connected:
                await asyncio.sleep(1)

        except Exception as exc:
            print(f"Error with {device_name} at {ble_device.address}: {exc}")
            print(f"Reconnecting to {device_name} in 5 s…")

        finally:
            if client and client.is_connected:
                try:
                    await client.disconnect()
                except Exception:
                    pass          # ignore double-disconnect errors

        await asyncio.sleep(5)   # small back-off before retry


# ---------------------------------------------------------------------
#  Activity merge helpers (unchanged)
# ---------------------------------------------------------------------
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")


def merge_esp_with_activity(esp_csv_path, activity_csv_path, categorized_csv_path):
    from datetime import timedelta

    activities = []
    with open(activity_csv_path, mode="r", newline="") as f_act:
        reader = csv.DictReader(f_act)
        for row in reader:
            try:
                start_dt = parse_datetime(row["activity_start_date"], row["activity_start_time"])
                end_dt   = parse_datetime(row["activity_end_date"],   row["activity_end_time"])
                label    = row["activity_label"]
                activities.append((start_dt, end_dt, label))
            except Exception as exc:
                print("Error parsing activity row:", exc)
                continue

    with open(esp_csv_path, mode="r", newline="") as f_esp, \
         open(categorized_csv_path, mode="w", newline="") as f_out:

        reader_esp  = csv.DictReader(f_esp)
        fieldnames  = reader_esp.fieldnames + ["activity_label"]
        writer_out  = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer_out.writeheader()

        for row in reader_esp:
            try:
                # Convert sensor's date/time
                sensor_dt = parse_datetime(row["Date"], row["Time"])
            except Exception as exc:
                print("Error parsing sensor row datetime:", exc)
                continue

            matched_label = None
            for (start_dt, end_dt, label) in activities:
                excluded_start = start_dt + timedelta(seconds=2)
                excluded_end   = end_dt   - timedelta(seconds=2)
                if excluded_start > excluded_end:
                    continue
                if excluded_start <= sensor_dt <= excluded_end:
                    matched_label = label
                    break

            if matched_label is not None:
                row["activity_label"] = matched_label
                writer_out.writerow(row)


# ---------------------------------------------------------------------
#  Main BLE entry-point
# ---------------------------------------------------------------------
async def main():
    initialize_csv_files()

    print("Scanning for BLE devices …")
    devices = await BleakScanner.discover(timeout=5.0)

    # Pick the first advertisement for every ESP-n we care about
    def pick(name_sub):
        return next((d for d in devices if d.name and name_sub in d.name), None)

    esp1 = pick("ESP32-1")
    esp2 = pick("ESP32-2")
    esp3 = pick("ESP32-3")
    esp4 = pick("ESP32-4")
    esp5 = pick("ESP32-5")

    if not any([esp1, esp2, esp3, esp4, esp5]):
        print("No ESP-32 devices found!")
        return

    print("BLE scan complete. Connecting …")
    tasks = []
    if esp1: tasks.append(asyncio.create_task(connect_and_listen(esp1.name, esp1, 1))); await asyncio.sleep(0.2)
    if esp2: tasks.append(asyncio.create_task(connect_and_listen(esp2.name, esp2, 2))); await asyncio.sleep(0.2)
    if esp3: tasks.append(asyncio.create_task(connect_and_listen(esp3.name, esp3, 3))); await asyncio.sleep(0.2)
    if esp4: tasks.append(asyncio.create_task(connect_and_listen(esp4.name, esp4, 4))); await asyncio.sleep(0.2)
    if esp5: tasks.append(asyncio.create_task(connect_and_listen(esp5.name, esp5, 5))); await asyncio.sleep(0.2)

    await asyncio.gather(*tasks)


# ---------------------------------------------------------------------
#  Flask routes (unchanged)
# ---------------------------------------------------------------------
# logging.getLogger('werkzeug').setLevel(logging.ERROR)  # Uncomment to mute Flask logs


@app.route('/data', methods=['GET'])
def get_data():
    async def fetch_data():
        async with data_lock:
            return {
                f"ESP{i+1}": {
                    "MPU1": {"ax": d[0],  "ay": d[1],  "az": d[2],
                             "gx": d[3],  "gy": d[4],  "gz": d[5]},
                    "MPU2": {"ax": d[6],  "ay": d[7],  "az": d[8],
                             "gx": d[9],  "gy": d[10], "gz": d[11]},
                    "HMC":  {"x":  d[12], "y":  d[13], "z": d[14],
                             "degrees": d[15]},
                    "Timestamps": {"date": d[16], "time": d[17]}
                }
                for i, d in enumerate(global_data.values()) if d
            }

    return jsonify(asyncio.run(fetch_data()))


@app.route('/activity', methods=['POST'])
def post_activity():
    global activity_data
    try:
        data = request.json
        required_keys = [
            "activity_label", "activity_start_date", "activity_start_time",
            "activity_end_date", "activity_end_time"
        ]
        if not all(k in data for k in required_keys):
            return jsonify({"status": "error", "message": "Missing required keys."}), 400

        activity_data = {k: data[k] for k in required_keys}
        append_activity_to_csv(activity_data)

        def do_merge_now():
            try:
                for i in range(1, 6):
                    merge_esp_with_activity(f"esp{i}.csv", "activity.csv",
                                            f"categorized_esp{i}.csv")
            except Exception as exc:
                print("Error in merging thread:", exc)

        Thread(target=do_merge_now, daemon=True).start()
        return jsonify({"status": "success", "updated_activity": activity_data}), 200

    except Exception as exc:
        print("Error processing POST /activity:", exc)
        return jsonify({"status": "error", "message": str(exc)}), 400


# ---------------------------------------------------------------------
#  Run Flask alongside asyncio loop
# ---------------------------------------------------------------------
def run_flask():
    app.run(host="0.0.0.0", port=5000, debug=False)


flask_thread = Thread(target=run_flask, daemon=True)
flask_thread.start()


# ---------------------------------------------------------------------
#  Kick everything off
# ---------------------------------------------------------------------
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nProgram terminated.")
