# Benchmark workflows (Vast PyWorker)

Vast runs `BenchmarkConfig` against one workflow per deployment. Pick the file with **`BENCHMARK_GENERATION_LANE`** (see `../worker.py` in this package), or set **`BENCHMARK_WORKFLOW_FILE`** to a filename in this directory.

Files use **`benchmark_<MODEL>_<…>.json`**:

| Default file | `BENCHMARK_GENERATION_LANE` values | Notes |
|--------------|--------------------------------------|--------|
| `benchmark_FLUX2_4090.json` | `FLUX2_4090` | Flux I2I-shaped graph from prod `workflows_api/images/nudify/nudify_api.json`, tuned lighter (**0.5** MP, **10** steps). S3: `BENCHMARK_IMAGE_*`. |
| `benchmark_WAN22_5090_5SEC.json` | `WAN22_5090`, `WAN22_5090_5SEC` | Wan I2V / `3some_cowgirl_5sec` topology; **96×144**, **5** WAN steps. |
| `benchmark_WAN22_5090_10SEC.json` | `WAN22_5090_10SEC` | Same as prod `3some_cowgirl_10sec_api.json`; **96×144**, **5** steps per sampler pair. |
| `benchmark_WAN22_5090_15SEC.json` | `WAN22_5090_15SEC` | Same as prod `3some_cowgirl_15sec_api.json`; **96×144**, **5** steps per sampler pair. |
| `benchmark_LTX23_5090_AI2V.json` | `LTX23_5090`, `LTX23_5090_AI2V` | LTX **2.3** AI2V from prod `workflows_api/AI2V/LTX2.3_AI2V_Audio_api.json`. **96×144** intake (same as WAN I2V benchmarks via `INTConstant` nodes 292/293). Image → `ETN_LoadImageBase64` (S3); audio → `Load Audio` node (S3 via `BENCHMARK_AUDIO_*` + `BENCHMARK_IMAGE_*`). |

If `BENCHMARK_GENERATION_LANE` is unset or not in the map, or the expected file is missing, the worker falls back to a minimal **Text2Image** benchmark (see `worker._fallback_benchmark_payload`).

Per-lane `VAST_WORKLOAD_UNITS_<generation_lane>`; no global. `comfy-vast-serverless/scripts/README.md`.
