# File: ble_json_server.py
import uasyncio as asyncio
import ujson
import bluetooth
import aioble
import time
from micropython import const

# ====== Config ======
DEVICE_NAME = "PicoJSON"

# UUIDs: must match the PC sender
SERVICE_UUID = bluetooth.UUID("6E400001-B5A3-F393-E0A9-E50E24DCCA9E")
RX_UUID      = bluetooth.UUID("6E400002-B5A3-F393-E0A9-E50E24DCCA9E")  # Write / Write Without Response
TX_UUID      = bluetooth.UUID("6E400003-B5A3-F393-E0A9-E50E24DCCA9E")  # Notify (optional ack)

# Watchdog: if no data received for this many ms, disconnect and re-advertise
PACKET_IDLE_TIMEOUT_MS = const(5000)

# Hard cap on RX buffer to prevent runaway memory usage (bytes)
BUFFER_HARD_LIMIT = const(64 * 1024)

# ====== GATT ======
service = aioble.Service(SERVICE_UUID)

# capture=True => rx_char.written() yields (conn, data)
# Allow BOTH write (with response) and write without response
rx_char = aioble.Characteristic(
    service,
    RX_UUID,
    read=False,
    write=True,
    write_no_response=True,   # <-- important for response=False writes from PC
    notify=False,
    capture=True
)

# Optional notify characteristic for ack/status back to the PC
tx_char = aioble.Characteristic(service, TX_UUID, read=False, write=False, notify=True)

aioble.register_services(service)

# ====== Streaming parser state ======
_buf = bytearray()
_expected_len = None

def _reset_stream():
    """Clear parser state."""
    global _buf, _expected_len
    _buf = bytearray()
    _expected_len = None

async def _handle_message(conn, payload: bytes):
    """
    Process one complete JSON message (bytes) and optionally send an ACK notify.
    """
    try:
        obj = ujson.loads(payload)
        print("Packet:", obj)
        # Optional: send a short ack back
        try:
            await tx_char.notify(conn, b'{"status":"ok"}')
        except Exception:
            # Not fatal if notify fails
            pass
    except Exception as e:
        print("JSON decode error:", e)
        try:
            await tx_char.notify(conn, b'{"status":"error","reason":"json"}')
        except Exception:
            pass

def _process_stream():
    """
    Consume as many complete frames as possible from _buf using:
        <4-byte little-endian length> + <payload>
    Returns: list[bytes] of complete payloads.
    """
    global _buf, _expected_len
    messages = []

    while True:
        # Need header?
        if _expected_len is None:
            if len(_buf) >= 4:
                _expected_len = int.from_bytes(_buf[0:4], "little")
                _buf = _buf[4:]
            else:
                break  # not enough for header yet

        # Need payload?
        if _expected_len is not None:
            if len(_buf) >= _expected_len:
                payload = bytes(_buf[:_expected_len])
                messages.append(payload)
                _buf = _buf[_expected_len:]
                _expected_len = None
                # Loop to see if another frame is already buffered
                continue
            else:
                break  # wait for more payload bytes

    return messages

async def _rx_task(conn, update_last_rx):
    """
    Receive writes on RX, accumulate into a stream buffer, and parse messages.
    `capture=True` means rx_char.written() returns (conn_obj, data).
    """
    global _buf
    _reset_stream()
    while True:
        res = await rx_char.written()  # (conn_obj, data) or just data on some builds
        try:
            conn_obj, data = res
        except Exception:
            # Fallback if your aioble build yields only data
            conn_obj = conn
            data = res

        # If multiple connections somehow arrive (shouldn't when bound to one), ignore others
        if conn_obj is not conn:
            # Defensive: skip data from other connections
            continue

        update_last_rx()

        # Append to buffer and guard against runaway growth
        _buf.extend(data)
        if len(_buf) > BUFFER_HARD_LIMIT:
            print("Buffer overflow; resetting stream.")
            _reset_stream()
            continue

        # Parse as many complete frames as possible
        for payload in _process_stream():
            await _handle_message(conn, payload)

async def _connection_handler(conn: aioble.Connection):
    """
    Handle a single connection: run RX task + watchdog. On watchdog expiry,
    disconnect so the PC can reconnect cleanly.
    """
    last_rx_ms = time.ticks_ms()

    def update_last_rx():
        nonlocal last_rx_ms
        last_rx_ms = time.ticks_ms()

    async def watchdog():
        nonlocal last_rx_ms
        try:
            while True:
                await asyncio.sleep_ms(250)
                if time.ticks_diff(time.ticks_ms(), last_rx_ms) > PACKET_IDLE_TIMEOUT_MS:
                    print("Watchdog: no packets; disconnecting to re-advertise")
                    try:
                        await conn.disconnect()
                    except Exception:
                        pass
                    return
        except asyncio.CancelledError:
            return

    rx = asyncio.create_task(_rx_task(conn, update_last_rx))
    wd = asyncio.create_task(watchdog())

    try:
        await conn.disconnected()
    finally:
        rx.cancel()
        wd.cancel()
        _reset_stream()
        print("Disconnected")

async def _advertise_forever():
    """
    Advertise, accept a connection, process it, then loop back to advertising.
    """
    while True:
        try:
            print("Advertising as", DEVICE_NAME)
            adv = aioble.advertise(
                interval_us=250_000,
                name=DEVICE_NAME,
                services=[SERVICE_UUID],
                appearance=0x0000,
                connectable=True,
            )
            conn = await adv
            print("Connected:", conn)
            await _connection_handler(conn)
        except KeyboardInterrupt:
            # Keep the server alive so the PC can reconnect again
            print("KeyboardInterrupt: restarting advertising …")
            await asyncio.sleep_ms(300)
        except Exception as e:
            print("Advertise/connection error:", e)
            await asyncio.sleep_ms(500)

try:
    asyncio.run(_advertise_forever())
finally:
    # Leave BLE in a sane state if the VM stops
    asyncio.new_event_loop()

