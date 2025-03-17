import asyncio
from bleak import BleakClient, BleakScanner

SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-123456789abc"


async def connect_and_listen_android(device_name, device_address):
    while True:
        try:
            async with BleakClient(device_address) as client:
                print(f"Connected to Android App ({device_address})")

                def notification_handler(sender, data):
                    try:
                        decoded_data = data.decode()
                        print(f"Received data from Android App: {decoded_data}")
                    except Exception as e:
                        print(f"Error processing data: {e}")

                await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
                print("Listening for data from Android App... Press Ctrl+C to exit.")

                while True:
                    await asyncio.sleep(1)
        except Exception as e:
            print(f"Error with Android App: {e}")
        print("Reconnecting to Android App...")
        await asyncio.sleep(5)


async def main():
    print("Scanning for BLE devices...")
    devices = await BleakScanner.discover()

    android_devices = [device for device in devices if "NisaTab" in device.name]

    if not android_devices:
        print("No Android App device found! Exiting...")
        return

    android_device = android_devices[0]
    print(f"Found Android App: {android_device.name} ({android_device.address})")

    await connect_and_listen_android(android_device.name, android_device.address)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nProgram terminated.")