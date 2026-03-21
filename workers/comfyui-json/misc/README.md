# Benchmark workflows (Vast PyWorker)

Vast runs `BenchmarkConfig` against one workflow per deployment. Pick the file with **`BENCHMARK_GENERATION_LANE`** (see `../worker.py` in this package), or set **`BENCHMARK_WORKFLOW_FILE`** to a filename in this directory.

| Lane | Default file | Notes |
|------|----------------|-------|
| `I2I_4090` | `benchmark_I2I_4090.json` | Same graph as prod `workflows_api/images/nudify/nudify_api.json` (Flux 2 Klein), tuned **lighter**: **0.5** megapixels scale, **10** scheduler steps. Input from S3 (`BENCHMARK_IMAGE_*`). |
| `I2V_5090_5SEC` | `benchmark_I2V_4090_5SEC.json` | 5090 **benchmark calibration** template lane (`vast_provision_benchmark_templates.py`). Same topology as prod `3some_cowgirl_5sec_api.json`; benchmark uses **96×144** spatial primitives and **5** WAN steps (heavier cold-start signal, target ~10–20s GPU class–dependent). Filename is historical (`4090` in the JSON name only). |
| `I2V_5090_FREE` | `benchmark_I2V_4090_5SEC.json` | 5090 **free-pool** prod template lane (boot benchmark). Bot sends `generation_lane=I2V_5090_5SEC` for workload. |
| `I2V_5090_PAID` | `benchmark_I2V_5090_15SEC.json` | 5090 **paid-pool** template lane (boot benchmark). Template env also carries `VAST_WORKLOAD_UNITS_I2V_5090_5SEC` (same value as free pool) plus 10s/15s keys. |
| `I2V_5090_10SEC` | `benchmark_I2V_5090_10SEC.json` | Same topology as prod `3some_cowgirl_10sec_api.json`; **96×144**, **5** steps per sampler pair (benchmark tuning). |
| `I2V_5090_15SEC` | `benchmark_I2V_5090_15SEC.json` | Same topology as prod `3some_cowgirl_15sec_api.json`; **96×144**, **5** steps per sampler pair (benchmark tuning). |

If `BENCHMARK_GENERATION_LANE` is unset or unknown, **`benchmark.json`** is used when present (legacy).

Template **`VAST_WORKLOAD_UNITS`** is required when the request has no `generation_lane`; per-lane traffic requires **`VAST_WORKLOAD_UNITS_<LANE>`** (no code defaults). See **`comfy-vast-serverless/docs/VAST_BENCHMARK_LANES_AND_BOT_COST.md`** (monorepo root).
