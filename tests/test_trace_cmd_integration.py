from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

import pytest

_PIPELINE_DAG = {
    "name": "pipeline",
    "tasks": [
        {"id": "ingest", "name": "Ingest"},
        {"id": "transform", "name": "Transform", "depends_on": ["ingest"]},
        {"id": "validate", "name": "Validate", "depends_on": ["transform"]},
        {"id": "load", "name": "Load", "depends_on": ["validate"]},
    ],
}


class TestTraceCmdIntegration:
    def _write_dag(self, data: dict) -> str:
        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(data, fh)
        fh.close()
        return fh.name

    def teardown_method(self, _method):
        # cleanup handled per test
        pass

    def test_trace_from_root_exits_zero(self):
        path = self._write_dag(_PIPELINE_DAG)
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pipecheck", "trace", path, "ingest"],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0
        finally:
            os.unlink(path)

    def test_trace_output_contains_task_ids(self):
        path = self._write_dag(_PIPELINE_DAG)
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pipecheck", "trace", path, "ingest"],
                capture_output=True,
                text=True,
            )
            assert "ingest" in result.stdout or "transform" in result.stdout
        finally:
            os.unlink(path)

    def test_trace_max_depth_limits_output(self):
        path = self._write_dag(_PIPELINE_DAG)
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pipecheck",
                    "trace",
                    path,
                    "ingest",
                    "--max-depth",
                    "1",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0
            assert "load" not in result.stdout
        finally:
            os.unlink(path)

    def test_missing_file_exits_nonzero(self):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipecheck",
                "trace",
                "/no/such/file.json",
                "ingest",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
