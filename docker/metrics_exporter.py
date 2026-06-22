from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import docker
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily


class DockerMetricsCollector:
    def __init__(self) -> None:
        self.client = docker.DockerClient(base_url="unix:///var/run/docker.sock")

    def collect(self) -> Iterator[GaugeMetricFamily]:
        cpu = GaugeMetricFamily(
            "news_flow_container_cpu_cores",
            "Container CPU usage measured in CPU cores.",
            labels=["service", "container"],
        )
        memory = GaugeMetricFamily(
            "news_flow_container_memory_bytes",
            "Container memory usage in bytes.",
            labels=["service", "container"],
        )

        containers = [
            container
            for container in self.client.containers.list()
            if container.labels.get("com.docker.compose.service")
        ]
        with ThreadPoolExecutor(max_workers=min(len(containers), 16) or 1) as executor:
            results = executor.map(self._read_container, containers)

        for result in results:
            if result is None:
                continue
            service, container_name, cpu_cores, working_set = result
            labels = [service, container_name]
            cpu.add_metric(labels, cpu_cores)
            memory.add_metric(labels, working_set)

        yield cpu
        yield memory

    @staticmethod
    def _read_container(container: docker.models.containers.Container):
        service = container.labels.get("com.docker.compose.service")
        if not service:
            return None
        try:
            stats = container.stats(stream=False)
        except docker.errors.DockerException:
            return None

        cpu_stats = stats.get("cpu_stats") or {}
        previous_cpu_stats = stats.get("precpu_stats") or {}
        cpu_delta = (
            (cpu_stats.get("cpu_usage") or {}).get("total_usage", 0)
            - (previous_cpu_stats.get("cpu_usage") or {}).get("total_usage", 0)
        )
        system_delta = (
            cpu_stats.get("system_cpu_usage", 0)
            - previous_cpu_stats.get("system_cpu_usage", 0)
        )
        online_cpus = cpu_stats.get("online_cpus") or len(
            (cpu_stats.get("cpu_usage") or {}).get("percpu_usage") or []
        )
        cpu_cores = (
            cpu_delta / system_delta * online_cpus
            if cpu_delta > 0 and system_delta > 0 and online_cpus
            else 0.0
        )

        memory_stats = stats.get("memory_stats") or {}
        memory_usage = memory_stats.get("usage", 0)
        inactive_file = (memory_stats.get("stats") or {}).get("inactive_file", 0)
        working_set = max(memory_usage - inactive_file, 0)
        return service, container.name, cpu_cores, working_set


if __name__ == "__main__":
    REGISTRY.register(DockerMetricsCollector())
    start_http_server(9101)
    while True:
        time.sleep(3600)
