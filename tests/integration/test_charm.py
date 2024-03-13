#!/usr/bin/env python3
# Copyright 2023 pguimaraes
# See LICENSE file for licensing details.

import asyncio
import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
MYSQL_APP_NAME = "mysql"
PGSQL_APP_NAME = "postgresql"


DB_CHARM = {
    "mysql": {
        "charm": "mysql",
        "channel": "8.0/edge",
        "config": {"profile": "testing"},
        "app_name": MYSQL_APP_NAME,
    },
    "pgsql": {
        "charm": "postgresql",
        "channel": "14/edge",
        "config": {},
        "app_name": PGSQL_APP_NAME,
    },
}


@pytest.mark.parametrize(
    "db_driver",
    [
        (pytest.param("mysql", marks=pytest.mark.group("mysql"))),
        (pytest.param("pgsql", marks=pytest.mark.group("postgresql"))),
    ],
)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, db_driver) -> None:
    """Build the charm and deploy + 3 mysql units to ensure a cluster is formed."""
    charm = await ops_test.build_charm(".")

    config = {
        "threads": 1,
        "tables": 1,
        "scale": 1,
        "driver": db_driver,
    }

    await asyncio.gather(
        ops_test.model.deploy(
            DB_CHARM[db_driver]["charm"],
            application_name=DB_CHARM[db_driver]["app_name"],
            num_units=3,
            channel=DB_CHARM[db_driver]["channel"],
            config=DB_CHARM[db_driver]["config"],
        ),
        ops_test.model.deploy(
            charm,
            application_name=APP_NAME,
            num_units=1,
            config=config,
        ),
    )

    await ops_test.model.relate(f"{APP_NAME}:database", f"{MYSQL_APP_NAME}:database")

    # Reduce the update_status frequency until the cluster is deployed
    async with ops_test.fast_forward("60s"):
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[APP_NAME].units) == 1
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, MYSQL_APP_NAME],
            status="active",
            raise_on_blocked=True,
            timeout=15 * 60,
        )
