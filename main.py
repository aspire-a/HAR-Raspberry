import asyncio
from bleak import BleakClient, BleakScanner
import json
import csv
from datetime import datetime

SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-123456789abc"


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


def append_to_csv(device_index, data):
    filename = f"esp{device_index}.csv"
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data)


async def connect_and_listen(device_name, device_address, device_index):
    """
    Connect to an ESP32 device and listen for BLE notifications indefinitely.
    """
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

                append_to_csv(device_index, row)

                print(f"{device_name} - Data: {row}")

            except Exception as e:
                print(f"{device_name} - Error processing data: {e}")

        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Listening to {device_name}... Press Ctrl+C to exit.")

        while True:
            await asyncio.sleep(1)


async def main():
    """
    Main function to scan and connect to ESP32 devices and keep listening concurrently.
    """
    initialize_csv_files()
    print("Scanning for BLE devices...")

    devices = await BleakScanner.discover()

    esp1_devices = [device for device in devices if "ESP32-1" in device.name]
    esp2_devices = [device for device in devices if "ESP32-2" in device.name]
    esp3_devices = [device for device in devices if "ESP32-3" in device.name]
    esp4_devices = [device for device in devices if "ESP32-4" in device.name]
    esp5_devices = [device for device in devices if "ESP32-5" in device.name]

    if not esp1_devices:
        print("No ESP32-1 devices found!")
    if not esp2_devices:
        print("No ESP32-2 devices found!")
    if not esp3_devices:
        print("No ESP32-3 devices found!")
    if not esp4_devices:
        print("No ESP32-4 devices found!")
    if not esp5_devices:
        print("No ESP32-5 devices found!")

    if not esp1_devices and not esp2_devices and not esp3_devices and not esp4_devices and not esp5_devices:
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


try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nProgram terminated.")