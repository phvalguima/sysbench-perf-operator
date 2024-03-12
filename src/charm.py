#!/usr/bin/env python3
# Copyright 2023 pguimaraes
# See LICENSE file for licensing details.

"""This connects the sysbench service to the database and the grafana agent.

The first action after installing the sysbench charm and relating it to the different
apps, is to prepare the db. The user must run the prepare action to create the database.

The prepare action will run the sysbench prepare command to create the database and, at its
end, it sets a systemd target informing the service is ready.

The next step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.
"""

import logging
import os
import shutil
import subprocess
from typing import Any, Dict, List
from pydantic import BaseModel, root_validator
from enum import Enum

import ops
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt
from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_restart,
    service_running,
    service_stop,
    service_failed,
)
from jinja2 import Environment, FileSystemLoader, exceptions
from ops.main import main

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


METRICS_PORT = 8088
TPCC_SCRIPT = "script"
SYSBENCH_SVC = "sysbench"
SYSBENCH_PATH = f"/etc/systemd/system/{SYSBENCH_SVC}.service"
LUA_SCRIPT_PATH = "/usr/share/sysbench/tpcc.lua"
SYSBENCH_SVC_READY_TARGET = f"{SYSBENCH_SVC}_prepared.target"

DATABASE_NAME = "sysbench-db"  # TODO: use a UUID here and publish its name in the peer relation

DATABASE_RELATION = "database"
COS_AGENT_RELATION = "cos-agent"
PEER_RELATION = "benchmark-peer"


class SysbenchError(Exception):
    """Sysbench error."""


class SysbenchExecFailedError(SysbenchError):
    """Sysbench execution failed error."""


class SysbenchIsInWrongStateError(SysbenchError):
    """Sysbench is in wrong state error."""

    def __init__(self, unit_state: SysbenchExecStatusEnum, app_state: SysbenchExecStatusEnum):
        self.unit_state = unit_state
        self.app_state = app_state
        super().__init__(f"Unit state: {unit_state}, App state: {app_state}")


class SysbenchMissingOptionsError(SysbenchError):
    """Sysbench missing options error."""


class SysbenchBaseDatabaseModel(BaseModel):
    """Sysbench database model.

    Holds all the details of the sysbench database.
    """
    host: str
    port: int
    unix_socket: str
    username: str
    password: str
    db_name: str
    tables: int
    scale: int

    @root_validator()
    @classmethod
    def validate_if_missing_params(cls, field_values):
        missing_param = []
        for f in ["username", "password", "db_name"]:
            if f not in field_values or field_values[f] is None:
                missing_param.append(f)
        if missing_param:
            raise SysbenchMissingOptionsError(f"{missing_param}")
        endpoint_set = (
            "host" in field_values and field_values["host"] is not None
            and "port" in field_values and field_values["port"] is not None
        ) or (
            "unix_socket" in field_values and field_values["unix_socket"] is not None
        )
        if not endpoint_set:
            raise SysbenchMissingOptionsError("Missing endpoint as unix_socket OR host:port")
        return field_values


class SysbenchExecutionModel(BaseModel):
    """Sysbench execution model.

    Holds all the details of the sysbench execution.
    """
    threads: int
    duration: int
    db_info: SysbenchBaseDatabaseModel


class SysbenchExecStatusEnum(Enum):
    """Sysbench execution status.

    The state-machine is the following:
    UNSET -> PREPARED -> RUNNING -> STOPPED -> UNSET

    ERROR can be set after any state apart from UNSET, PREPARED, STOPPED.   

    UNSET means waiting for prepare to be executed. STOPPED means the sysbench is ready
    but the service is not running. 
    """
    UNSET = "unset"
    PREPARED = "prepared"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


class SysbenchService:
    """Represents the sysbench service."""

    def __init__(
        self,
        svc_name: str = SYSBENCH_SVC,
        ready_target: str = SYSBENCH_SVC_READY_TARGET,
    ):
        self.svc = svc_name
        self.ready_target = ready_target

    def render_service_file(self, labels) -> bool:
        # Render the systemd service file
        db = SysbenchExecutionModel()
        _render(
            "sysbench.service.j2",
            f"/etc/systemd/system/{self.svc}.service",
            {
                "db_driver": "mysql",
                "threads": db.threads,
                "tables": db.db_info.tables,
                "scale": db.db_info.scale,
                "db_name": db.db_info.db_name,
                "db_user": db.db_info.username,
                "db_password": db.db_info.password,
                "db_host": db.db_info.host,
                "db_port": db.db_info.port,
                "duration": db.duration,
                "extra_labels": labels,
            },
        )
        return daemon_reload()

    def is_prepared(self) -> bool:
        try:
            return "active" in subprocess.check_output(
                [
                    "systemctl",
                    "is-active",
                    self.ready_target,
                ],
                text=True,
            )
        except Exception:
            return False

    def finished_preparing(self) -> bool:
        """Wraps the prepare step by setting the prepared target."""
        try:
            shutil.copyfile(f"templates/{self.ready_target}", f"/etc/systemd/system/{self.ready_target}")
            return daemon_reload()
        except Exception:
            return False

    def is_running(self) -> bool:
        return self.is_prepared() and service_running(self.svc)

    def is_stopped(self) -> bool:
        return self.is_prepared() and not self.is_running() and not self.is_failed()

    def is_failed(self) -> bool:
        return self.is_prepared() and service_failed(self.svc)

    def run(self) -> bool:
        if self.is_stopped() or self.is_failed():
            return service_restart(self.svc)
        return self.is_running()

    def stop(self) -> bool:
        if not self.is_stopped() and not self.is_failed():
            return service_stop(self.svc)
        return self.is_stopped()

    def unset(self) -> bool:
        try:
            result = self.stop()
            os.remove(f"/etc/systemd/system/{self.ready_target}")
            return daemon_reload() and result
        except Exception:
            pass


class SysbenchStatus:
    """Renders the sysbench status updates the relation databag"""

    def __init__(self, charm: ops.charm.CharmBase, relation: str, svc: SysbenchService):
        self.charm = charm
        self.svc = svc
        self.relation = relation

    def _relation(self) -> Dict[str, Any]:
        return self.charm.model.get_relation(self.relation)

    def app_status(self) -> SysbenchExecStatusEnum:
        """Returns the status."""
        if not self._relation:
            return None
        return SysbenchExecStatusEnum(
            self._relation.data[self.charm.app].get("status", SysbenchExecStatusEnum.UNSET.value)
        )

    def unit_status(self) -> SysbenchExecStatusEnum:
        if not self._relation:
            return None
        return SysbenchExecStatusEnum(
            self._relation.data[self.charm.unit].get("status", SysbenchExecStatusEnum.UNSET.value)
        )

    def set(self, status: SysbenchExecStatusEnum) -> None:
        if not self._relation:
            return
        if self.charm.is_leader():
            self._relation.data[self.charm.app]["status"] = status
        self._relation.data[self.charm.unit]["status"] = status

    def _has_error_happend(self) -> bool:
        for unit in self._relation.units:
            if self._relation.data[unit].get("status") == SysbenchExecStatusEnum.ERROR.value:
                return True
        return (
            self.unit_status() == SysbenchExecStatusEnum.ERROR
            or self.app_status() == SysbenchExecStatusEnum.ERROR
        )

    def service_status(self) -> SysbenchExecStatusEnum:
        if not self.svc.is_prepared():
            return SysbenchExecStatusEnum.UNSET
        if self.svc.is_failed():
            return SysbenchExecStatusEnum.ERROR
        if self.svc.is_running():
            return SysbenchExecStatusEnum.RUNNING
        if self.svc.is_stopped():
            return SysbenchExecStatusEnum.STOPPED
        return SysbenchExecStatusEnum.PREPARED

    def check(self) -> SysbenchExecStatusEnum:
        """Implements the state machine.

        This charm will also update the databag accordingly. It is built of three
        different data sources: this unit last status (from relation), app status and
        the current status of the sysbench service.
        """
        if self._has_error_happend():
            raise SysbenchExecFailedError()

        if self.charm.is_leader():
            # Either we are waiting for PREPARE to happen, or it has happened, as
            # the prepare command runs synchronously with the charm. Check if the
            # target exists:
            self.set(self.service_status())
            return self.service_status()

        # Now, we need to execute the unit state
        self.set(self.service_status())
        # If we have a failure, then we should react to it
        if self.service_status() == SysbenchExecStatusEnum.ERROR:
            raise SysbenchExecFailedError()
        if self.service_status() != self.app_status():
            raise SysbenchIsInWrongStateError(self.service_status(), self.app_status())
        return self.service_status()


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


class SysbenchOptionsFactory:
    """Renders the database options and abstracts the main charm from the db type details.

    It uses the data coming from both relation and config.
    """
    def __init__(self, relation_data: Dict[str, Any], config: Dict[str, Any]):
        self.relation_data = relation_data
        self.config = config

    def get_database_options(self) -> Dict[str, Any]:
        """Returns the database options."""
        endpoints = list(self.relation_data.values())[0].get("endpoints")

        unix_socket, host, port = None, None, None
        if endpoints.startswith("file://"):
            unix_socket = endpoints[7:]
        else:
            host, port = endpoints.split(":")

        return SysbenchBaseDatabaseModel(
            host=host,
            port=port,
            unix_socket=unix_socket,
            user=self.relation_data.get("username"),
            password=self.relation_data.get("password"),
            tables=self.config.get("tables"),
            scale=self.config.get("scale"),
        )

    def get_execution_options(self) -> Dict[str, Any]:
        """Returns the execution options."""
        return SysbenchExecutionModel(
            threads=self.config.get("threads"),
            duration=self.config.get("duration"),
            db_info=self.get_database_options()
        )


class SysbenchPerfOperator(ops.CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.prepare_action, self.on_prepare_action)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)

        self.framework.observe(self.on[PEER_RELATION].relation_joined, self._on_peer_changed)
        self.framework.observe(self.on[PEER_RELATION].relation_changed, self._on_peer_changed)

        self.database = DatabaseRequires(self, DATABASE_RELATION, DATABASE_NAME)
        self.framework.observe(
            getattr(self.database.on, "endpoints_changed"), self._on_endpoints_changed
        )
        self.framework.observe(
            self.on[DATABASE_RELATION].relation_broken, self._on_relation_broken
        )
        self._grafana_agent = COSAgentProvider(
            self,
            scrape_configs=self.scrape_config,
            refresh_events=[],
        )
        self.sysbench_status = SysbenchStatus(self, PEER_RELATION, SysbenchService())
        self.labels = ",".join([self.model.name, self.unit.name])

    def _set_charm_status(self) -> SysbenchExecStatusEnum:
        """Recovers the sysbench status."""
        status = self.sysbench_status.check()
        if status == SysbenchExecStatusEnum.ERROR:
            self.unit.status = ops.model.BlockedStatus("Sysbench failed, please check logs")
        elif status == SysbenchExecStatusEnum.UNSET:
            self.unit.status = ops.model.ActiveStatus()
        if status == SysbenchExecStatusEnum.PREPARED:
            self.unit.status = ops.model.WaitingStatus("Sysbench is prepared: execute run to start")
        if status == SysbenchExecStatusEnum.RUNNING:
            self.unit.status = ops.model.ActiveStatus("Sysbench is running")
        if status == SysbenchExecStatusEnum.STOPPED:
            self.unit.status = ops.model.WaitingStatus("User requested sysbench run to halt")

    def __del__(self):
        """Set status for the operator and finishes the service."""
        self.unit.status = self._set_charm_status()

    @property
    def is_tls_enabled(self):
        """Return tls status."""
        return False

    @property
    def _unit_ip(self) -> str:
        """Current unit ip."""
        return self.model.get_binding(COS_AGENT_RELATION).network.bind_address

    def _on_config_changed(self, _):
        # For now, ignore the configuration
        svc = SysbenchService()
        if not svc.is_running():
            # Nothing to do, there was no setup yet
            return
        svc.stop()
        svc.render_service_file(self.labels)
        svc.run()

    def _on_relation_broken(self, _):
        SysbenchService().stop()

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

    def _on_peer_changed(self, _):
        """Peer relation changed."""
        if (
            not self.is_leader()
            and self.sysbench_status.app_status() == SysbenchExecStatusEnum.PREPARED
            and self.sysbench_status.service_status() not in [SysbenchExecStatusEnum.PREPARED, SysbenchExecStatusEnum.RUNNING]
        ):
            # We need to mark this unit as prepared so we can rerun the script later
            self.sysbench_status.set(SysbenchExecStatusEnum.PREPARED)

    def _execute_sysbench_cmd(self, extra_labels, command: str, driver: str, script: str = LUA_SCRIPT_PATH):
        """Execute the sysbench command."""
        db = SysbenchOptionsFactory()
        output = subprocess.check_output(
            [
                "/usr/bin/sysbench_svc.py",
                f"--tpcc_script={script}",
                f"--db_driver={driver}",
                f"--threads={db.threads}",
                f"--tables={db.db_info.tables}",
                f"--scale={db.db_info.scale}",
                f"--db_name={db.db_info.db_name}",
                f"--db_user={db.db_info.username}",
                f"--db_password={db.db_info.password}",
                f"--db_host={db.db_info.host}",
                f"--db_port={db.db_info.port}",
                f"--duration={db.duration}",
                f"--command={command}",
                f"--extra_labels={extra_labels}",
            ],
            timeout=86400,
        )
        logger.debug("Sysbench output: %s", output)

    def on_prepare_action(self, event):
        """Prepare the database.

        There are two steps: the actual prepare command and setting a target to inform the
        prepare was successful.
        """
        if not self.is_leader():
            event.fail("Failed: only leader can prepare the database")
            return
        if not self.sysbench_status.check() != SysbenchExecStatusEnum.UNSET:
            event.fail("Failed: sysbench is already prepared")
        self.unit.status = ops.model.MaintenanceStatus("Running prepare command...")
        SysbenchService().finished_preparing()
        self.sysbench_status.set(SysbenchExecStatusEnum.PREPARED)
        event.set_results({"status": "prepared"})

    def on_run_action(self, event):
        """Run benchmark action."""
        if self.sysbench_status.check() == SysbenchExecStatusEnum.ERROR:
            logger.warning("Overriding ERROR status and restarting service")
        elif self.sysbench_status.check() not in [
            SysbenchExecStatusEnum.PREPARED,
            SysbenchExecStatusEnum.STOPPED,
        ]:
            event.fail("Failed: sysbench is not prepared")
            return

        self.unit.status = ops.model.MaintenanceStatus("Setting up benchmark")
        svc = SysbenchService()
        svc.stop()
        svc.render_service_file(self.labels)
        svc.run()
        self.sysbench_status.set(SysbenchExecStatusEnum.RUNNING)
        event.set_results({"status": "running"})

    def on_benchmark_stop_action(self, event):
        """Stop benchmark service."""
        if self.sysbench_status.check() != SysbenchExecStatusEnum.RUNNING:
            event.fail("Failed: sysbench is not running")
            return
        svc = SysbenchService()
        svc.stop()
        self.sysbench_status.set(SysbenchExecStatusEnum.STOPPED)
        event.set_results({"status": "stopped"})

    def on_clean_action(self, event):
        if not self.is_leader():
            event.fail("Failed: only leader can prepare the database")
            return
        if self.sysbench_status.check() == SysbenchExecStatusEnum.UNSET:
            event.fail("Nothing to do, sysbench units are idle")
            return
        if self.sysbench_status.check() == SysbenchExecStatusEnum.RUNNING:
            SysbenchService().stop()
            logger.info("Sysbench service stopped in clean action")
        self.unit.status = ops.model.MaintenanceStatus("Cleaning up database")

    def _on_endpoints_changed(self, _) -> None:
        # TODO: update the service if it is already running
        pass


if __name__ == "__main__":
    main(SysbenchPerfOperator)
