import asyncio
from bleak import BleakClient, BleakScanner
import json
import csv
from datetime import datetime
from flask import Flask, request, jsonify
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
activity_data = None  # Global variable to store activity data
data_lock = asyncio.Lock()  # For thread-safe access to global_data

# Initialize CSV files for each ESP device and activity
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
        headers = ["activity_label", "activity_start_date", "activity_start_time", "activity_end_date", "activity_end_time"]
        writer.writerow(headers)

# Append data to the respective CSV file
def append_to_csv(device_index, data):
    filename = f"esp{device_index}.csv"
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data)

# Append activity data to activity.csv
def append_activity_to_csv(activity):
    with open("activity.csv", mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            activity["activity_label"],
            activity["activity_start_date"],
            activity["activity_start_time"],
            activity["activity_end_date"],
            activity["activity_end_time"]
        ])

# Update global data asynchronously
async def update_global_data(key, value):
    async with data_lock:
        global_data[key] = value

async def connect_and_listen(device_name, device_address, device_index):
    while True:
        try:
            async with BleakClient(device_address) as client:
                print(f"Connected to {device_name} at {device_address}")

                def notification_handler(sender, data):
                    try:
                        decoded_data = data.decode()
                        sensor_data = json.loads(decoded_data)

                        now = datetime.now()
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
                        # print(f"{device_name} - Data written to esp{device_index}.csv.")

                    except Exception as e:
                        print(f"{device_name} - Error processing data: {e}")

                await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
                # print(f"Listening to {device_name}... Press Ctrl+C to exit.")

                while client.is_connected:
                    await asyncio.sleep(1)

        except Exception as e:
            print(f"Error with {device_name} at {device_address}: {e}")
            print(f"Reconnecting to {device_name}...")

        await asyncio.sleep(5)







def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")


def merge_esp_with_activity(esp_csv_path, activity_csv_path, categorized_csv_path):
    """
    1) Loads all activity ranges from activity_csv_path
    2) Reads esp_csv_path and checks if each row's timestamp is within any activity's [start, end].
    3) Writes matching rows to categorized_csv_path with an added 'activity_label' column.
    """

    from datetime import timedelta

    # --- Step A: Read activity.csv into a list of (start_dt, end_dt, activity_label)
    activities = []
    with open(activity_csv_path, mode="r", newline="") as f_act:
        reader = csv.DictReader(f_act)
        for row in reader:
            try:
                start_dt = parse_datetime(row["activity_start_date"], row["activity_start_time"])
                end_dt   = parse_datetime(row["activity_end_date"], row["activity_end_time"])
                label    = row["activity_label"]
                activities.append((start_dt, end_dt, label))
            except Exception as e:
                print("Error parsing activity row:", e)
                continue

    # --- Step B: Read the sensor CSV (espX.csv) and prepare to write to categorized_espX.csv
    with open(esp_csv_path, mode="r", newline="") as f_esp, \
         open(categorized_csv_path, mode="w", newline="") as f_out:

        reader_esp = csv.DictReader(f_esp)
        fieldnames = reader_esp.fieldnames + ["activity_label"]  # add the label column
        writer_out = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer_out.writeheader()

        for row in reader_esp:
            try:
                # Convert sensor's date/time
                sensor_dt = parse_datetime(row["Date"], row["Time"])
            except Exception as e:
                print("Error parsing sensor row datetime:", e)
                continue

            matched_label = None

            for (start_dt, end_dt, label) in activities:
                # Shift each activity by 2 seconds
                excluded_start = start_dt + timedelta(seconds=2)
                excluded_end   = end_dt - timedelta(seconds=2)

                # If the activity is shorter than 4 seconds total, skip
                if excluded_start > excluded_end:
                    continue

                if excluded_start <= sensor_dt <= excluded_end:
                    matched_label = label
                    break

            if matched_label is not None:
                row["activity_label"] = matched_label
                writer_out.writerow(row)





async def main():
    initialize_csv_files()

    print("Scanning for BLE devices...")
    devices = await BleakScanner.discover()

    esp1_devices = [device for device in devices if "ESP32-1" in device.name]
    esp2_devices = [device for device in devices if "ESP32-2" in device.name]
    esp3_devices = [device for device in devices if "ESP32-3" in device.name]
    esp4_devices = [device for device in devices if "ESP32-4" in device.name]
    esp5_devices = [device for device in devices if "ESP32-5" in device.name]

    if not any([esp1_devices, esp2_devices, esp3_devices, esp4_devices, esp5_devices]):
        print("No devices found!")
        return

    print("BLE scan complete. Connecting to devices...")
    tasks = []

    if esp1_devices:
        for device in esp1_devices:
            tasks.append(asyncio.create_task(connect_and_listen(device.name, device.address, 1)))
            await asyncio.sleep(1)
    if esp2_devices:
        for device in esp2_devices:
            tasks.append(asyncio.create_task(connect_and_listen(device.name, device.address, 2)))
            await asyncio.sleep(1)
    if esp3_devices:
        for device in esp3_devices:
            tasks.append(asyncio.create_task(connect_and_listen(device.name, device.address, 3)))
            await asyncio.sleep(1)
    if esp4_devices:
        for device in esp4_devices:
            tasks.append(asyncio.create_task(connect_and_listen(device.name, device.address, 4)))
            await asyncio.sleep(1)
    if esp5_devices:
        for device in esp5_devices:
            tasks.append(asyncio.create_task(connect_and_listen(device.name, device.address, 5)))
            await asyncio.sleep(1)

    await asyncio.gather(*tasks)

@app.route('/data', methods=['GET'])
def get_data():
    async def fetch_data():
        async with data_lock:
            formatted_data = {
                f"ESP{i+1}": {
                    "MPU1": {
                        "ax": data[0], "ay": data[1], "az": data[2],
                        "gx": data[3], "gy": data[4], "gz": data[5]
                    },
                    "MPU2": {
                        "ax": data[6], "ay": data[7], "az": data[8],
                        "gx": data[9], "gy": data[10], "gz": data[11]
                    },
                    "HMC": {
                        "x": data[12], "y": data[13], "z": data[14], "degrees": data[15]
                    },
                    "Timestamps": {
                        "date": data[16], "time": data[17]
                    }
                } for i, data in enumerate(global_data.values()) if data
            }
            return formatted_data

    data = asyncio.run(fetch_data())
    return jsonify(data)

@app.route('/activity', methods=['POST'])
def post_activity():
    global activity_data
    try:
        data = request.json
        required_keys = ["activity_label", "activity_start_date", "activity_start_time", "activity_end_date", "activity_end_time"]

        if not all(key in data for key in required_keys):
            return jsonify({"status": "error", "message": "Missing required keys."}), 400

        # Update global activity_data
        activity_data = {
            "activity_label": data["activity_label"],
            "activity_start_date": data["activity_start_date"],
            "activity_start_time": data["activity_start_time"],
            "activity_end_date": data["activity_end_date"],
            "activity_end_time": data["activity_end_time"]
        }

        append_activity_to_csv(activity_data)  # Write to activity.csv

        # print("Activity data updated:", activity_data)

        def do_merge_now():
            try:
                # Merge each espX.csv with the entire activity.csv
                for i in range(1, 6):
                    esp_file = f"esp{i}.csv"
                    categorized_file = f"categorized_esp{i}.csv"
                    merge_esp_with_activity(esp_file, "activity.csv", categorized_file)
                # print("Merging complete.")
            except Exception as e:
                print("Error in merging thread:", e)

        Thread(target=do_merge_now, daemon=True).start()

        return jsonify({"status": "success", "updated_activity": activity_data}), 200
    except Exception as e:
        print("Error processing POST request for activity:", e)
        return jsonify({"status": "error", "message": str(e)}), 400

# Flask server runner
def run_flask():
    app.run(host="0.0.0.0", port=5000, debug=False)

# Run Flask in a separate thread to avoid blocking the asyncio event loop
flask_thread = Thread(target=run_flask, daemon=True)
flask_thread.start()

# Execute the main function
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nProgram terminated.")
