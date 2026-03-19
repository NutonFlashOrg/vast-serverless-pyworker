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

# Custom backend writes "Backend ready"; stock uses "To see the GUI go to: "
MODEL_LOAD_LOG_MSG = ["Backend ready"]
MODEL_ERROR_LOG_MSGS = [
    "MetadataIncompleteBuffer",
    "Value not in list: ",
    "[ERROR] Provisioning Script failed",
    "Error:",
    "Traceback (most recent call last):",
]


def _get_benchmark_workflow_path() -> Path | None:
    """Resolve benchmark workflow path: misc/benchmark.json only."""
    misc_path = Path(__file__).resolve().parent / "misc" / "benchmark.json"
    if misc_path.is_file():
        _log.info("Benchmark workflow: misc/benchmark.json (using)")
        return misc_path
    _log.warning("No benchmark workflow found (misc/benchmark.json absent)")
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
    bucket = os.getenv("BENCHMARK_IMAGE_BUCKET") or os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME")
    key = (os.getenv("BENCHMARK_IMAGE_KEY") or "").strip()
    if bucket and key:
        input_images.append({"bucket": bucket, "key": key})

    # Randomize seed to avoid ComfyUI cache reuse and produce realistic benchmark timings
    for node in workflow.values() if isinstance(workflow, dict) else []:
        if (
            isinstance(node, dict)
            and node.get("class_type") == "PrimitiveInt"
            and (node.get("_meta") or {}).get("title") == "Seed"
        ):
            node.setdefault("inputs", {})["value"] = random.randint(
                -(2**63), 2**63 - 1
            )
            break

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


def workload_calculator(payload: dict) -> float:
    return 100.0


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

Worker(worker_config).run()
