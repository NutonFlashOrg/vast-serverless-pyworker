"""
Vast PyWorker for ComfyUI with app-format request transform.

Accepts app format: {workflow, input_images, user_id, generation_id, ...}
Transforms to Vast format and forwards to API wrapper.
"""

import logging
import os
import random
import sys
import uuid
from pathlib import Path

from vastai import BenchmarkConfig, HandlerConfig, LogActionConfig, Worker, WorkerConfig

for _name in ("botocore", "boto3", "s3transfer", "urllib3"):
    logging.getLogger(_name).setLevel(logging.WARNING)

_log = logging.getLogger("comfyui-json")

# API wrapper config (our backend on 8189; stock ComfyUI uses 18288)
MODEL_SERVER_URL = os.getenv("MODEL_SERVER_URL", "http://127.0.0.1")
MODEL_SERVER_PORT = int(os.getenv("MODEL_SERVER_PORT", "8189"))
MODEL_LOG_FILE = os.getenv("MODEL_LOG_FILE", "/app/logs/backend.log")
MODEL_HEALTHCHECK_ENDPOINT = os.getenv("MODEL_HEALTHCHECK_ENDPOINT", "/health")
BENCHMARK_RUNS = int(os.getenv("BENCHMARK_RUNS", "1"))

# Lane → default benchmark JSON under misc/ (set BENCHMARK_GENERATION_LANE per Vast template)
_DEFAULT_BENCHMARK_FILES: dict[str, str] = {
    "I2I_4090": "benchmark_I2I_4090.json",
    "I2V_4090_5SEC": "benchmark_I2V_4090_5SEC.json",
    "I2V_5090_10SEC": "benchmark_I2V_5090_10SEC.json",
    "I2V_5090_15SEC": "benchmark_I2V_5090_15SEC.json",
}
# Default request load when VAST_WORKLOAD_UNITS is unset. Used by workload_calculator for Vast
# routing/scaling (see comfy-vast-serverless/docs/VAST_BENCHMARK_LANES_AND_BOT_COST.md).
#
# These are bootstrap values only: if the lane benchmark is much lighter than prod, measured
# worker perf will be optimistic unless load is calibrated (typically set VAST_WORKLOAD_UNITS per
# template from T_prod_median / T_bench_median). Bot VAST_REQUEST_COST_* (credits) is a separate
# contract and may intentionally differ after calibration.
_DEFAULT_WORKLOAD_BY_LANE: dict[str, float] = {
    "I2I_4090": 100.0,
    "I2V_4090_5SEC": 200.0,
    "I2V_5090_10SEC": 350.0,
    "I2V_5090_15SEC": 500.0,
}

# Custom backend writes "Backend ready"; stock uses "To see the GUI go to: "
MODEL_LOAD_LOG_MSG = ["Backend ready"]
MODEL_ERROR_LOG_MSGS = [
    "MetadataIncompleteBuffer",
    "Value not in list: ",
    "[ERROR] Provisioning Script failed",
    "Error:",
    "Traceback (most recent call last):",
]


def _normalized_benchmark_lane() -> str:
    return (
        (os.getenv("BENCHMARK_GENERATION_LANE") or "").strip().upper().replace(" ", "_")
    )


def _get_benchmark_workflow_path() -> Path | None:
    """Resolve benchmark JSON: BENCHMARK_WORKFLOW_FILE, then lane default, then benchmark.json."""
    misc_dir = Path(__file__).resolve().parent / "misc"
    override = (os.getenv("BENCHMARK_WORKFLOW_FILE") or "").strip()
    if override:
        p = misc_dir / override
        if p.is_file():
            _log.info("Benchmark workflow: %s (BENCHMARK_WORKFLOW_FILE)", p.name)
            return p
        _log.warning("BENCHMARK_WORKFLOW_FILE=%s not found under misc/", override)

    lane = _normalized_benchmark_lane()
    if lane and lane in _DEFAULT_BENCHMARK_FILES:
        p = misc_dir / _DEFAULT_BENCHMARK_FILES[lane]
        if p.is_file():
            _log.info("Benchmark workflow lane=%s file=%s", lane, p.name)
            return p
        _log.warning("Lane %s expects %s but file missing", lane, p.name)

    legacy = misc_dir / "benchmark.json"
    if legacy.is_file():
        _log.info("Benchmark workflow: misc/benchmark.json (legacy default)")
        return legacy
    _log.warning("No benchmark workflow found under misc/")
    return None


def _get_benchmark_payload() -> dict:
    """Generate benchmark payload. App format is transformed to workflow_json so backend receives
    the same format as production (benchmark bypasses request_parser, so we transform here)."""
    import json

    path = _get_benchmark_workflow_path()
    if path is None:
        return _fallback_benchmark_payload()
    with open(path, encoding="utf-8") as f:
        workflow = json.load(f)

    if "workflow" in workflow:
        workflow = workflow["workflow"]
    input_images: list[dict] = []
    bucket = (
        os.getenv("BENCHMARK_IMAGE_BUCKET")
        or os.getenv("S3_BUCKET")
        or os.getenv("S3_BUCKET_NAME")
    )
    key = (os.getenv("BENCHMARK_IMAGE_KEY") or "").strip()
    if bucket and key:
        input_images.append({"bucket": bucket, "key": key})

    # Randomize seeds to avoid ComfyUI cache reuse and produce realistic benchmark timings
    if isinstance(workflow, dict):
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            cls = node.get("class_type")
            meta = node.get("_meta") or {}
            inputs = node.setdefault("inputs", {})
            if cls == "PrimitiveInt" and meta.get("title") == "Seed":
                inputs["value"] = random.randint(-(2**63), 2**63 - 1)
            if cls == "RandomNoise" and "noise_seed" in inputs:
                inputs["noise_seed"] = random.randint(0, 2**63 - 1)

    # App format; transform to workflow_json so backend receives correct format
    app_payload = {
        "input": {
            "workflow": workflow,
            "user_id": "bench",
            "generation_id": f"bench-{uuid.uuid4().hex}",
            "timeout": 300,
            "input_images": input_images,
            "watermark_enabled": False,
        }
    }
    from .workflow_transform import transform_app_to_vast

    return transform_app_to_vast(app_payload)


def _fallback_benchmark_payload() -> dict:
    """Fallback when no custom benchmark workflow; use stock Text2Image modifier."""
    return {
        "input": {
            "request_id": f"bench-{random.randint(1000, 99999)}",
            "modifier": "Text2Image",
            "modifications": {
                "prompt": "a beautiful sunset over mountains, digital art, highly detailed",
                "width": 512,
                "height": 512,
                "steps": 20,
                "seed": random.randint(0, sys.maxsize),
            },
        }
    }


def request_parser(json_msg: dict) -> dict:
    """Transform app format to Vast format before forwarding to API wrapper."""
    from .workflow_transform import transform_app_to_vast

    return transform_app_to_vast(json_msg)


def _workload_units() -> float:
    raw = os.getenv("VAST_WORKLOAD_UNITS")
    if raw is not None and str(raw).strip() != "":
        try:
            return float(raw)
        except ValueError:
            _log.warning("Invalid VAST_WORKLOAD_UNITS=%r; using lane/default", raw)
    lane = _normalized_benchmark_lane()
    if lane and lane in _DEFAULT_WORKLOAD_BY_LANE:
        return _DEFAULT_WORKLOAD_BY_LANE[lane]
    return 100.0


def workload_calculator(payload: dict) -> float:
    """Return declared load for this worker's route; Vast combines this with benchmark-derived perf."""
    return _workload_units()


worker_config = WorkerConfig(
    model_server_url=MODEL_SERVER_URL,
    model_server_port=MODEL_SERVER_PORT,
    model_log_file=MODEL_LOG_FILE,
    model_healthcheck_url=MODEL_HEALTHCHECK_ENDPOINT,
    handlers=[
        HandlerConfig(
            route="/generate/sync",
            allow_parallel_requests=False,
            max_queue_time=60.0,
            workload_calculator=workload_calculator,
            request_parser=request_parser,
            benchmark_config=BenchmarkConfig(
                generator=_get_benchmark_payload,
                runs=BENCHMARK_RUNS,
                concurrency=1,
            ),
        ),
    ],
    log_action_config=LogActionConfig(
        on_load=MODEL_LOAD_LOG_MSG,
        on_error=MODEL_ERROR_LOG_MSGS,
    ),
)

if __name__ == "__main__":
    Worker(worker_config).run()
