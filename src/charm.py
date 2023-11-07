#!/usr/bin/env python3
# Copyright 2023 pguimaraes
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

import subprocess
import logging
import os
import shutil
from typing import Any, Dict, List

import ops
from ops.main import main
from ops.charm import CharmEvents
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1.systemd import daemon_reload, service_restart, service_stop
from jinja2 import Environment, FileSystemLoader, exceptions

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


METRICS_PORT = 8088
TPCC_SCRIPT = "script"
SYSBENCH_SVC = "sysbench"
SYSBENCH_PATH = f"/etc/systemd/system/{SYSBENCH_SVC}.service"
DATABASE_NAME = "sysbench-db"
DATABASE_RELATION = "database"


def _render(src_template_file: str, dst_filepath: str, values: Dict[str, Any]):
    templates_dir = os.path.join(os.environ.get("CHARM_DIR", ""), "templates")
    template_env = Environment(loader=FileSystemLoader(templates_dir))
    try:
        template = template_env.get_template(src_template_file)
        content = template.render(values)
    except exceptions.TemplateNotFound as e:
        raise e
    # save the file in the destination
    with open(dst_filepath, "w") as f:
        f.write(content)
        os.chmod(dst_filepath, 0o640)


def _get_ip():
    """Read files to quickly figure out its own IP. It should work for any Ubuntu-based image."""
    with open("/etc/hosts") as f:
        hosts = f.read()
    with open("/etc/hostname") as f:
        hostname = f.read().replace("\n", "")
    line = [ln for ln in hosts.split("\n") if ln.find(hostname) >= 0][0]
    return line.split("\t")[0]


class SysbenchPerfOperator(ops.CharmBase):
    """Charm the service."""

    on = CharmEvents()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        # self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.sysbench_run_action, self.on_benchmark_run_action)

        self.database = DatabaseRequires(self, DATABASE_RELATION, DATABASE_NAME)
        self.framework.observe(
            getattr(self.database.on, "endpoints_changed"), self._on_endpoints_changed
        )
        self.framework.observe(
            self.on[DATABASE_RELATION].relation_broken, self._on_relation_broken
        )
        self._grafana_agent = COSAgentProvider(
            self,
            metrics_endpoints=[{"path": "/metrics", "port": METRICS_PORT}],
            scrape_configs=self.scrape_config,
            refresh_events=[],
        )

    @property
    def is_tls_enabled(self):
        """Return tls status."""
        return False

    @property
    def _unit_ip(self) -> str:
        """Current unit ip."""
        return _get_ip()

    def _on_relation_broken(self, _):
        service_stop(SYSBENCH_SVC)

    def scrape_config(self) -> List[Dict]:
        """Generate scrape config for the Patroni metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self._unit_ip}:{METRICS_PORT}"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "https" if self.is_tls_enabled else "http",
            }
        ]

    def _on_install(self, _):
        """Installs the basic packages and python dependencies.

        No exceptions are captured as we need all the dependencies below to even start running.
        """
        apt.update()
        apt.add_package(["sysbench", "python3-prometheus-client", "python3-jinja2", "unzip"])
        shutil.copyfile("templates/sysbench_svc.py", "/usr/bin/sysbench_svc.py")
        os.chmod("/usr/bin/sysbench_svc.py", 0o700)

    def on_benchmark_run_action(self, event):
        """Run benchmark action."""
        duration = event.params.get("duration", 0)
        # copy the tpcc file
        tpcc_filepath = self.model.resources.fetch(TPCC_SCRIPT)
        try:
            subprocess.check_output(["unzip", "-o", "-j", tpcc_filepath, "-d", "/usr/share/sysbench/"])
        except Exception as e:
            raise e

        if not os.path.exists("/usr/share/sysbench/tpcc.lua"):
            raise Exception()

        db = self._database_config

        # Render the systemd service file
        _render(
            "sysbench.service.j2",
            SYSBENCH_PATH,
            {
                "db_driver": "mysql",
                "threads": 8,
                "tables": 10,
                "scale": 10,
                "db_name": DATABASE_NAME,
                "db_user": db["user"],
                "db_password": db["password"],
                "db_host": db["host"],
                "db_port": db["port"],
                "duration": duration,
                "extra_labels": ",".join([self.model.name, self.unit.name]),
            },
        )
        # Reload and restart service now
        daemon_reload()
        service_restart(SYSBENCH_SVC)

    def on_benchmark_stop_action(self, _):
        """Stop benchmark service."""
        service_stop(SYSBENCH_SVC)

    def _on_endpoints_changed(self, _) -> None:
        # TODO: update the service if it is already running
        pass

    @property
    def _database_config(self):
        """Returns the database config to use to connect to the MySQL cluster."""
        # identify the database relation
        data = list(self.database.fetch_relation_data().values())[0]

        username, password, endpoints = (
            data.get("username"),
            data.get("password"),
            data.get("endpoints"),
        )
        if None in [username, password, endpoints]:
            return {}

        config = {
            "user": username,
            "password": password,
            "database": DATABASE_NAME,
        }
        if endpoints.startswith("file://"):
            config["unix_socket"] = endpoints[7:]
        else:
            host, port = endpoints.split(":")
            config["host"] = host
            config["port"] = port

        return config


if __name__ == "__main__":
    main(SysbenchPerfOperator)