import os
import subprocess
from abc import abstractmethod
import datetime
from pathlib import Path
import shlex
import sys
import time
import csv
import logging
import concurrent.futures
from typing import List

import psutil


logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

executor = concurrent.futures.ThreadPoolExecutor()


class Metric:
    def __init__(self):
        self._values: List[float] = []

    @abstractmethod
    def get_value(self) -> float: ...

    def append_value(self, f):
        self._values.append(f.result())

    def generate(self):
        f = executor.submit(self.get_value)
        f.add_done_callback(self.append_value)

    @property
    def max(self) -> float:
        return max(self._values)

    @property
    def min(self) -> float:
        return min(self._values)

    @property
    def average(self) -> float:
        return round(sum(self._values) / len(self._values), 2)


class CPU(Metric):
    def get_value(self) -> float:
        return psutil.cpu_percent()


class Memory(Metric):
    def get_value(self) -> float:
        return psutil.virtual_memory().percent


class SwapMemory(Metric):
    def get_value(self) -> float:
        return psutil.swap_memory().percent


class DiskUsage(Metric):
    def get_value(self) -> float:
        return psutil.disk_usage(os.getcwd()).percent


class DiskReadTime(Metric):
    def get_value(self) -> float:
        return psutil.disk_io_counters().read_time


class DiskWriteTime(Metric):
    def get_value(self) -> float:
        return psutil.disk_io_counters().write_time


class BootTime(Metric):
    def get_value(self) -> float:
        return psutil.boot_time()


class Temperature(Metric):
    def get_value(self) -> float:
        if os.name == 'nt':
            return 0.0
        temperatures = psutil.sensors_temperatures()
        return 0.0  # TODO


class FanSpeed(Metric):
    def get_value(self) -> float:
        if os.name == 'nt':
            return 0.0
        fan_speeds = psutil.sensors_fans()
        fan = list(fan_speeds.values())[0]
        return 0.0  # TODO


def write_output_to_file(command: str, path: Path):
    cmd = shlex.split(command)
    logger.debug("Writing output of command %s to file %s", command, path)
    with path.open("w") as f:
        subprocess.run(cmd, stdout=f)


def timestamp(dt) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def get_filename(prefix: str, dt: datetime.datetime, extension: str = "txt") -> str:
    return f"{prefix}_{timestamp(dt)}.{extension}"


def top(base_dir: Path, dt: datetime.datetime):
    if os.name == "nt":
        cmd = 'powershell "ps | sort -des cpu"'
    else:
        cmd = 'top -b -n 1'
    top_dir = base_dir / "top"
    top_dir.mkdir(parents=True, exist_ok=True)
    path = top_dir / get_filename("top", dt)
    write_output_to_file(cmd, path)


headers = ["Timestamp", "CPU", "Memory", "Swap Memory", "Disk Read Time", "Disk Write Time", "Disk Usage", "Boot Time"]
if os.name == "posix":
    headers += ["Temperature", "Fan Speed"]


def run(output_dir: Path):
    logger.info("Running with directory %s", output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dt = datetime.datetime.now()
    top(output_dir, dt)
    logger.info("Top command finished")
    results = [dt.strftime('%x %X')]
    metrics = [m() for m in [CPU, Memory, SwapMemory, DiskReadTime, DiskWriteTime]]
    logger.info('Gathering metrics')
    for i in range(0, 5):
        logger.debug("Gathering metrics %d", i)
        for m in metrics:
            m.generate()
        time.sleep(1)
    for m in metrics:
        results.append(m.average)
    one_time_metrics = [m() for m in [DiskUsage, BootTime]]
    logger.info("Gathering one-time metrics %s", one_time_metrics)
    if os.name == "posix":
        one_time_metrics += [Temperature, FanSpeed]
    for m in one_time_metrics:
        results.append(m.get_value())
    stats_file = output_dir / "stats.csv"
    logger.info("Using csv file %s", stats_file)
    exists = stats_file.exists()
    with stats_file.open('a', newline="") as f:
        writer = csv.writer(f)
        if not exists:
            logger.info("Writing header row %s", headers)
            writer.writerow(headers)
        logger.info("Writing results %s", results)
        writer.writerow(results)
        logger.info('Done')


if __name__ == "__main__":
    output_dir = Path(sys.argv[1])
    run(output_dir)
