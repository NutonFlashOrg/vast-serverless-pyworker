"""
Transform app request format to Vast /generate/sync format.
Aligns with RunPod serverless: client sends workflow + S3 refs, worker downloads,
injects base64, patches, forwards to backend for execution, watermarking, S3 upload.

Accepts: {workflow, input_images, user_id, generation_id, watermark_enabled?, watermark_filename?, ...}
Produces: {request_id, workflow_json, run_subdir, user_id, generation_id,
          watermark_enabled, watermark_filename, timeout, s3?: {...}} for backend.
"""

import base64
import copy
import logging
import os
import re
import uuid
from pathlib import Path

logger = logging.getLogger("workflow_transform")

# S3: Vast uses S3_BUCKET_NAME; we also accept S3_BUCKET for compatibility
def _get_s3_config() -> dict | None:
    bucket = os.getenv("S3_BUCKET_NAME") or os.getenv("S3_BUCKET")
    access = os.getenv("S3_ACCESS_KEY_ID")
    secret = os.getenv("S3_SECRET_ACCESS_KEY")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    region = os.getenv("S3_REGION", "us-east-1")
    if not all([bucket, access, secret, endpoint]):
        return None
    return {
        "bucket": bucket,
        "access_key_id": access,
        "secret_access_key": secret,
        "endpoint_url": endpoint,
        "region": region,
    }


def _safe_component(s: str) -> str:
    s = str(s or "")
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("_")
    return s[:80] if s else "x"


def _make_job_subdir(user_id: str, generation_id: str, job_id: str | None) -> str:
    prefix = os.getenv("JOB_PREFIX", "vast")
    rid_src = (job_id or "").strip()
    rid = _safe_component(rid_src[:12]) if rid_src else uuid.uuid4().hex[:12]
    return f"{prefix}/u{_safe_component(user_id)}/g{_safe_component(generation_id)}/{rid}"


def _download_input_images(
    input_images: list[dict],
    input_dir: Path,
) -> list[tuple[str, Path]]:
    """Download images from S3. Returns [(title, path), ...]."""
    import boto3
    from botocore.config import Config

    cfg = _get_s3_config()
    if not cfg:
        raise RuntimeError("S3 not configured for input_images download")
    client = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=cfg["access_key_id"],
        aws_secret_access_key=cfg["secret_access_key"],
        region_name=cfg["region"],
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )
    input_dir.mkdir(parents=True, exist_ok=True)
    results: list[tuple[str, Path]] = []
    for i, entry in enumerate(input_images):
        bucket = entry.get("bucket")
        key = entry.get("key")
        title = (entry.get("title") or "").strip()
        if not bucket or not key:
            raise RuntimeError(f"input_images[{i}] missing bucket or key")
        ext = Path(key).suffix or ".jpg"
        safe_name = _safe_component(title) if title else f"input_{i}"
        local_path = (input_dir / f"{safe_name}{ext}").resolve()
        if not str(local_path).startswith(str(input_dir.resolve())):
            raise RuntimeError("Invalid input path traversal")
        client.download_file(bucket, key, str(local_path))
        logger.info("Downloaded %s/%s -> %s", bucket, key, local_path)
        results.append((title, local_path))
    return results


def _patch_workflow(
    workflow: dict,
    run_subdir: str,
    job_input: dict,
    downloaded_images: list[tuple[str, Path]],
) -> dict:
    """Patch workflow: sageattn, VHS_VideoCombine, ETN_LoadImageBase64, prompt."""
    wf = copy.deepcopy(workflow)
    for node in wf.values():
        if isinstance(node, dict) and (node.get("inputs") or {}).get("attention_override") == "sageattn":
            node.setdefault("inputs", {})["attention_override"] = "none"
    for node in wf.values():
        if isinstance(node, dict) and node.get("class_type") == "VHS_VideoCombine":
            node.setdefault("inputs", {})["filename_prefix"] = f"{run_subdir}/result"
            node.setdefault("inputs", {})["save_output"] = True
    etn_nodes: list[tuple[str, str, dict]] = []
    for nid, node in wf.items():
        if isinstance(node, dict) and node.get("class_type") == "ETN_LoadImageBase64":
            title = (node.get("_meta") or {}).get("title") or ""
            etn_nodes.append((nid, title.strip(), node))
    img_by_title = {t: p for t, p in downloaded_images if t}
    img_no_title = [p for t, p in downloaded_images if not t]
    for nid, ntitle, node in etn_nodes:
        local = img_by_title.get(ntitle) if ntitle else (img_no_title.pop(0) if img_no_title else None)
        if local and local.exists():
            b64 = base64.b64encode(local.read_bytes()).decode("utf-8")
            node.setdefault("inputs", {})["image"] = b64
    if downloaded_images:
        for nid, _, node in etn_nodes:
            if not (node.get("inputs") or {}).get("image"):
                raise RuntimeError(f"Failed to inject image into ETN node {nid}")
    prompt_title = (job_input.get("prompt_node_title") or "").strip()
    user_prompt = (job_input.get("user_prompt") or "").strip()
    if prompt_title and user_prompt:
        for node in wf.values():
            if isinstance(node, dict) and node.get("class_type") == "CLIPTextEncode":
                if ((node.get("_meta") or {}).get("title") or "").strip() == prompt_title:
                    node.setdefault("inputs", {})["text"] = user_prompt
                    break
    return wf


def transform_app_to_vast(payload: dict) -> dict:
    """
    Transform app format to Vast API wrapper format.
    If already in Vast format (workflow_json present), return as-is.
    """
    inp = payload.get("input", payload)
    if isinstance(inp, dict) and "workflow_json" in inp:
        return payload
    workflow = inp.get("workflow") if isinstance(inp, dict) else None
    if not isinstance(workflow, dict):
        return payload
    input_images = (inp.get("input_images") or []) if isinstance(inp, dict) else []
    user_id = str(inp.get("user_id") or "")
    generation_id = str(inp.get("generation_id") or "")
    job_id = str(payload.get("id") or inp.get("request_id") or "")
    request_id = job_id or str(uuid.uuid4())
    run_subdir = _make_job_subdir(user_id, generation_id, job_id)
    job_input = dict(inp) if isinstance(inp, dict) else {}
    downloaded: list[tuple[str, Path]] = []
    if input_images:
        input_dir = Path("/tmp/input") / run_subdir
        downloaded = _download_input_images(input_images, input_dir)
    patched = _patch_workflow(workflow, run_subdir, job_input, downloaded)
    s3_cfg = _get_s3_config()
    s3_block = {}
    if s3_cfg:
        s3_block = {
            "access_key_id": s3_cfg["access_key_id"],
            "secret_access_key": s3_cfg["secret_access_key"],
            "endpoint_url": s3_cfg["endpoint_url"],
            "bucket_name": s3_cfg["bucket"],
            "region": s3_cfg["region"],
        }
    out_input: dict = {
        "request_id": request_id,
        "workflow_json": patched,
        "run_subdir": run_subdir,
        "user_id": user_id,
        "generation_id": generation_id,
        "timeout": int(job_input.get("timeout", 600)),
        "watermark_enabled": bool(job_input.get("watermark_enabled", True)),
        "watermark_filename": (job_input.get("watermark_filename") or "").strip() or None,
    }
    if s3_block:
        out_input["s3"] = s3_block
    return {"input": out_input}
