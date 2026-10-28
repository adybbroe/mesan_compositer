#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Application-level smoke tests for the command-line interfaces."""

import subprocess
import sys

import pytest


@pytest.mark.system
@pytest.mark.parametrize(
    "module_name",
    [
        "mesan_compositer.make_cloud_composite",
        "mesan_compositer.ct_quicklooks",
        "mesan_compositer.ctth_quicklooks",
        "mesan_compositer.mesan_composite_runner",
    ],
)
def test_command_line_module_help(module_name):
    """Check that important command-line modules start and expose help."""
    result = subprocess.run(
        [sys.executable, "-m", module_name, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout.lower()


@pytest.mark.system
@pytest.mark.parametrize(
    "command",
    [
        "make_cloud_composite",
        "prt_nwcsaf_cloudamount",
        "prt_nwcsaf_cloudheight",
        "ct_quicklooks",
        "ctth_quicklooks",
        "mesan_composite_runner",
    ],
)
def test_installed_console_scripts_expose_help(command):
    """Check that installed console entry points start and expose help."""
    result = subprocess.run(
        [command, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout.lower()
