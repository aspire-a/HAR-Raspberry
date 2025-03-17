import asyncio
from bleak import BleakClient, BleakScanner

SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-123456789abc"


async def main():
    # ESP32'yi Tara ve Bağlan
    print("Scanning for BLE devices...")
    devices = await BleakScanner.discover()
    esp_device = None
    for device in devices:
        if "ESP32-2" in device.name:
            esp_device = device
            break

    if not esp_device:
        print("ESP32 could not be found!")
        return

    async with BleakClient(esp_device.address) as client:
        print(f"Connected to ESP32: {esp_device.address}")

        def notification_handler(sender, data):
            print(f"Incoming data: {data.decode()}")

        # Bildirimleri Dinle
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print("Data listening... Ctr+c to exit.")

        while True:
            await asyncio.sleep(1)


asyncio.run(main())