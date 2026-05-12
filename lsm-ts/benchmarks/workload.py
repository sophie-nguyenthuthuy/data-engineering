"""Realistic IoT ingestion workload generator.

Models a fleet of sensors (temperature, humidity, pressure, power_consumption)
reporting at 1-second intervals.  Timestamps are nanoseconds since epoch.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass

from lsm.types import DataPoint, TSKey, TSValue


@dataclass
class SensorConfig:
    metric: str
    unit: str
    base: float
    noise: float  # ± noise/2


SENSORS = [
    SensorConfig("temperature",     "°C",  22.0, 4.0),
    SensorConfig("humidity",        "%",   55.0, 20.0),
    SensorConfig("pressure",        "hPa", 1013.0, 10.0),
    SensorConfig("power_draw",      "W",   120.0, 50.0),
    SensorConfig("vibration_rms",   "g",   0.05, 0.03),
    SensorConfig("co2_ppm",         "ppm", 420.0, 80.0),
]

FACILITIES = ["plant-A", "plant-B", "plant-C"]
MACHINES_PER_FACILITY = 20


def _device_tags(facility: str, machine_id: int) -> dict[str, str]:
    return {"facility": facility, "machine": f"m{machine_id:03d}"}


def generate_batch(
    num_points: int,
    start_ts_ns: int | None = None,
    interval_ns: int = 1_000_000_000,  # 1 second
    seed: int | None = 42,
) -> list[DataPoint]:
    """
    Generate *num_points* data points in chronological order.
    Simulates many devices reporting different metrics.
    """
    if seed is not None:
        random.seed(seed)
    if start_ts_ns is None:
        start_ts_ns = int(time.time_ns()) - num_points * interval_ns

    devices = [
        (facility, mid)
        for facility in FACILITIES
        for mid in range(MACHINES_PER_FACILITY)
    ]
    num_devices = len(devices)  # 60 devices
    num_sensors = len(SENSORS)

    points: list[DataPoint] = []
    ts = start_ts_ns
    for i in range(num_points):
        device_idx = i % num_devices
        sensor_idx = (i // num_devices) % num_sensors
        facility, machine_id = devices[device_idx]
        sensor = SENSORS[sensor_idx]

        # Small drift to simulate realistic sensor behavior
        drift = (random.random() - 0.5) * sensor.noise
        value = sensor.base + drift

        tags = _device_tags(facility, machine_id)
        key = TSKey.make(sensor.metric, tags, ts)
        point = DataPoint(key=key, value=TSValue(value=value))
        points.append(point)

        if device_idx == num_devices - 1:
            ts += interval_ns

    return points


def generate_out_of_order_batch(
    num_points: int,
    jitter_ns: int = 5_000_000_000,  # 5s jitter
    seed: int = 42,
) -> list[DataPoint]:
    """Like generate_batch but with late-arrival jitter (common in IoT)."""
    points = generate_batch(num_points, seed=seed)
    random.seed(seed)
    for p in points:
        jitter = random.randint(-jitter_ns, jitter_ns)
        new_ts = max(0, p.key.timestamp_ns + jitter)
        object.__setattr__(p.key, "timestamp_ns", new_ts)
    return points
