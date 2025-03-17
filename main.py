import asyncio
from bleak import BleakClient, BleakScanner

SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-123456789abc"


async def connect_and_listen(device, device_name):
    """Connect to an ESP32 device and listen for notifications."""
    async with BleakClient(device.address) as client:
        print(f"{device_name} bağlanıldı: {device.address}")

        def notification_handler(sender, data):
            try:

                decoded_data = data.decode()
                print(f"{device_name} - Gelen veri: {decoded_data}")
            except Exception as e:
                print(f"{device_name} - Veri çözümleme hatası: {e}")

        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"{device_name} veri dinleniyor... Çıkmak için Ctrl+C basın.")

        while True:
            await asyncio.sleep(1)


async def main():
    print("BLE cihazlarını tarıyor...")
    devices = await BleakScanner.discover()

    esp_devices = [device for device in devices if "ESP32-BLE" in device.name]
    if len(esp_devices) < 2:
        print("Yeterli ESP32 cihazı bulunamadı!")
        return

    device_1, device_2 = esp_devices[:2]
    print(f"ESP32 cihazları bulundu: {device_1.name}, {device_2.name}")

    task_1 = connect_and_listen(device_1, "ESP32-1")
    task_2 = connect_and_listen(device_2, "ESP32-2")

    await asyncio.gather(task_1, task_2)


try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nProgram sonlandırıldı.")