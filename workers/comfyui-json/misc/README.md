# Benchmark workflows (Vast PyWorker)

Vast runs `BenchmarkConfig` against one workflow per deployment. Pick the file with **`BENCHMARK_GENERATION_LANE`** (see `../worker.py` in this package), or set **`BENCHMARK_WORKFLOW_FILE`** to a filename in this directory.

| Lane | Default file | Notes |
|------|----------------|-------|
| `I2I_4090` | `benchmark_I2I_4090.json` | Same graph as prod `workflows_api/images/nudify/nudify_api.json` (Flux 2 Klein). Input image from S3 (`BENCHMARK_IMAGE_*`). |
| `I2V_4090_5SEC` | `benchmark_I2V_4090_5SEC.json` | Same graph as prod `3some_cowgirl_5sec_api.json`; **128×192**, **2** total WAN steps (KSampler 0→1, 1→2). |
| `I2V_5090_5SEC` | `benchmark_I2V_4090_5SEC.json` | Same file as `I2V_4090_5SEC` (5090 free I2V template lane). |
| `I2V_5090_PAID` | `benchmark_I2V_5090_15SEC.json` | Heaviest graph for unified paid I2V template (worker readiness / perf signal). |
| `I2V_5090_10SEC` | `benchmark_I2V_5090_10SEC.json` | Same graph as prod `3some_cowgirl_10sec_api.json` (incl. Sage + extend loop); **128×192**, **2** steps per sampler pair. |
| `I2V_5090_15SEC` | `benchmark_I2V_5090_15SEC.json` | Same graph as prod `3some_cowgirl_15sec_api.json`; **128×192**, **2** steps per sampler pair. |

If `BENCHMARK_GENERATION_LANE` is unset or unknown, **`benchmark.json`** is used when present (legacy).

Template **`VAST_WORKLOAD_UNITS`** is required when the request has no `generation_lane`; per-lane traffic requires **`VAST_WORKLOAD_UNITS_<LANE>`** (no code defaults). See **`comfy-vast-serverless/docs/VAST_BENCHMARK_LANES_AND_BOT_COST.md`** (monorepo root).
