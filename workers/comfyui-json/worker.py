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
    "I2V_5090_5SEC": "benchmark_I2V_4090_5SEC.json",
    "I2V_5090_PAID": "benchmark_I2V_5090_15SEC.json",
    "I2V_5090_10SEC": "benchmark_I2V_5090_10SEC.json",
    "I2V_5090_15SEC": "benchmark_I2V_5090_15SEC.json",
}
# Lanes that may appear on input.generation_lane (bot, benchmarks, legacy). Each requires
# VAST_WORKLOAD_UNITS_<LANE> when present — no numeric defaults (see VAST_BENCHMARK_LANES_AND_BOT_COST.md).
_KNOWN_WORKLOAD_LANES: frozenset[str] = frozenset(_DEFAULT_BENCHMARK_FILES.keys())

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

    if isinstance(workflow, dict):
        try:
            from .workflow_transform import randomize_workflow_seeds
        except ImportError:
            from workflow_transform import randomize_workflow_seeds

        randomize_workflow_seeds(workflow)

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
    try:
        from .workflow_transform import transform_app_to_vast
    except ImportError:
        # calibrate_workload_timing.py does ``import worker`` with comfyui-json on sys.path only
        from workflow_transform import transform_app_to_vast

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


def _normalize_lane_token(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "_")


def workload_calculator(payload: dict) -> float:
    """Declared load for Vast routing/scaling — must match bot SDK ``cost=`` for the same lane.

    With ``input.generation_lane``: requires ``VAST_WORKLOAD_UNITS_<LANE>`` (no defaults).
    Without ``generation_lane`` (benchmark / legacy): requires template ``VAST_WORKLOAD_UNITS``.
    """
    inp = payload.get("input")
    if isinstance(inp, dict):
        lane = _normalize_lane_token(str(inp.get("generation_lane") or ""))
        if lane:
            if lane not in _KNOWN_WORKLOAD_LANES:
                raise ValueError(
                    f"workload_calculator: unknown generation_lane={lane!r}; "
                    f"expected one of {sorted(_KNOWN_WORKLOAD_LANES)}"
                )
            env_key = f"VAST_WORKLOAD_UNITS_{lane}"
            raw = os.getenv(env_key)
            if raw is None or str(raw).strip() == "":
                raise RuntimeError(
                    f"Missing required environment variable {env_key} "
                    f"(generation_lane={lane} on request)"
                )
            try:
                return float(raw)
            except ValueError as e:
                raise ValueError(f"Invalid {env_key}={raw!r}") from e
    raw = os.getenv("VAST_WORKLOAD_UNITS")
    if raw is None or str(raw).strip() == "":
        raise RuntimeError(
            "Missing required environment variable VAST_WORKLOAD_UNITS "
            "(no input.generation_lane on request)"
        )
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"Invalid VAST_WORKLOAD_UNITS={raw!r}") from e


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
