#!/usr/bin/python3
# Copyright 2023 pguimaraes
# See LICENSE file for licensing details.

"""This method runs the sysbench call, collects its output and forwards to prometheus."""

import argparse
import signal
import subprocess

from prometheus_client import Gauge, start_http_server


class SysbenchService:
    """Sysbench service class."""

    def __init__(
        self,
        tpcc_script: str,
        threads: int,
        tables: int,
        scale: int,
        db_driver: str,
        db_name: str,
        db_user: str,
        db_password: str,
        db_host: str,
        db_port: int = 3306,
        duration: int = 0,
    ):
        self.tpcc_script = tpcc_script
        self.sysbench = f"sysbench {tpcc_script} --threads={threads} --tables={tables} --scale={scale} --force_pk=1 --db-driver={db_driver} --report_csv=no --time={duration}"
        if db_driver == "mysql":
            self.sysbench += f"--mysql-db={db_name} --mysql-user={db_user} --mysql-password={db_password} --mysql-host={db_host} --mysql-port={db_port}"
        else:
            raise Exception("Wrong db driver chosen")

    def _exec(self, cmd):
        subprocess.check_output(self.sysbench.split(" ") + cmd)

    def prepare(self):
        """Prepare the sysbench output."""
        return self._exec(["prepare"])

    def _process_line(self, line):
        return {
            "tps": line.split("tps: ")[1].split()[0],
            "qps": line.split("qps: ")[1].split()[0],
            "95p_latency": line.split("blabla: ")[1].split()[0],
        }

    def run(self, proc, metrics, label, extra_labels):
        """Run one step of the main sysbench service loop."""
        try:
            outs, errs = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            raise Exception("Timed out waiting for sysbench.")
        if errs:
            raise Exception(f"Error generated: {errs}")
        for line in outs.split("\n"):
            value = self._process_line(line)
            for m in ["tps", "qps", "95p_latency"]:
                self.add_benchmark_metric(
                    metrics, f"{label}_{m}", extra_labels, f"tpcc metrics for {m}", value[m]
                )

    def stop(self, proc):
        """Stop the service with SIGTERM."""
        proc.terminate()

    def clean(self):
        """Clean the sysbench database."""
        self._exec(["cleanup"])


def add_benchmark_metric(metrics, label, extra_labels, description, value):
    """Add the benchmark to the prometheus metric.

    labels:
        tpcc_{db_driver}_{tps|qps|95p_latency}
    """
    if label not in metrics:
        metrics[label] = Gauge(label, description, ["model", "unit"])
    metrics[label].labels(*extra_labels).set(value)


def main(args):
    """Run main method."""
    keep_running = True

    def _exit():
        keep_running = False  # noqa: F841

    svc = SysbenchService(*args)

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)
    start_http_server(8088)

    proc = subprocess.Popen(
        svc.sysbench.split(" ") + ["run"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    metrics = {}

    while keep_running:
        if args.command == "prepare":
            svc.prepare()
            keep_running = False  # Gracefully shutdown
        elif args.command == "run":
            svc.run(proc, metrics, f"tpcc_{args.db_driver}", args.extra_labels)
        elif args.command == "clean":
            svc.clean()
        else:
            raise Exception(f"Command option {args.command} not known")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="sysbench_svc", description="Runs the sysbench command as an argument."
    )
    parser.add_argument("--tpcc_script", type=str, help="Path to the tpcc lua script.")
    parser.add_argument("--db_driver", type=str, help="")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--tables", type=int, default=10)
    parser.add_argument("--scale", type=int, default=10)
    parser.add_argument("--db_name", type=str)
    parser.add_argument("--db_password", type=str)
    parser.add_argument("--db_host", type=str)
    parser.add_argument("--db_port", type=int)
    parser.add_argument("--duration", type=int)
    parser.add_argument("--command", type=str)
    parser.add_argument(
        "--extra_labels", type=str, help="comma-separated list of extra labels to be used."
    )

    args = parser.parse_args()

    main(args)
