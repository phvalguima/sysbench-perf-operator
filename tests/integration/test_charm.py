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


@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build the charm and deploy + 3 mysql units to ensure a cluster is formed."""
    charm = await ops_test.build_charm(".")

    await asyncio.gather(
        ops_test.model.deploy(
            MYSQL_APP_NAME,
            application_name=MYSQL_APP_NAME,
            num_units=3,
            channel="8.0/edge",
            config={"profile": "testing"},
        ),
        ops_test.model.deploy(
            charm,
            application_name=APP_NAME,
            num_units=1,
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
