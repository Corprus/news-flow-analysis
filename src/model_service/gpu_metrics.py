from __future__ import annotations

from collections.abc import Iterator
from threading import Lock

from prometheus_client.core import GaugeMetricFamily

try:
    import pynvml
except ImportError:  # CPU workers and lightweight test environments.
    pynvml = None


class NvidiaGpuCollector:
    def __init__(self) -> None:
        self._initialized = False
        self._initialization_lock = Lock()

    def _initialize(self) -> bool:
        if pynvml is None:
            return False
        with self._initialization_lock:
            if self._initialized:
                return True
            try:
                pynvml.nvmlInit()
            except pynvml.NVMLError:
                return False
            self._initialized = True
            return True

    def collect(self) -> Iterator[GaugeMetricFamily]:
        utilization = GaugeMetricFamily(
            "news_flow_gpu_utilization_ratio",
            "NVIDIA GPU utilization ratio.",
            labels=["index", "name", "uuid"],
        )
        memory_used = GaugeMetricFamily(
            "news_flow_gpu_memory_used_bytes",
            "NVIDIA GPU memory currently used in bytes.",
            labels=["index", "name", "uuid"],
        )
        memory_total = GaugeMetricFamily(
            "news_flow_gpu_memory_total_bytes",
            "Total NVIDIA GPU memory in bytes.",
            labels=["index", "name", "uuid"],
        )
        temperature = GaugeMetricFamily(
            "news_flow_gpu_temperature_celsius",
            "NVIDIA GPU temperature in degrees Celsius.",
            labels=["index", "name", "uuid"],
        )

        if not self._initialize():
            return

        try:
            device_count = pynvml.nvmlDeviceGetCount()
            for index in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                labels = [
                    str(index),
                    str(pynvml.nvmlDeviceGetName(handle)),
                    str(pynvml.nvmlDeviceGetUUID(handle)),
                ]
                gpu_utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
                memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
                utilization.add_metric(labels, gpu_utilization.gpu / 100)
                memory_used.add_metric(labels, memory.used)
                memory_total.add_metric(labels, memory.total)
                temperature.add_metric(
                    labels,
                    pynvml.nvmlDeviceGetTemperature(
                        handle,
                        pynvml.NVML_TEMPERATURE_GPU,
                    ),
                )
        except pynvml.NVMLError:
            return

        yield utilization
        yield memory_used
        yield memory_total
        yield temperature
