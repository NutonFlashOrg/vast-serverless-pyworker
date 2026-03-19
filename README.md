# PyWorker Fork for ComfyUI App-Format Support

Fork of [vast-ai/pyworker](https://github.com/vast-ai/pyworker) with request transformation for app-specific payloads.

## Features

- **Request parsing**: Accepts app format `{workflow, input_images, user_id, generation_id, ...}` and transforms to Vast `{workflow_json, s3, request_id}` before forwarding to the API wrapper.
- **S3 input images**: Downloads `input_images` from S3 and injects base64 into `ETN_LoadImageBase64` nodes.
- **Workflow patching**: Applies sageattn override, VHS_VideoCombine prefix, prompt injection.
- **Benchmark**: Uses `misc/benchmark.json` with optional S3-backed input image.

## Usage

Set in Vast template env:

- `PYWORKER_REPO=https://github.com/yourorg/pyworker`
- `PYWORKER_REF=main`

## Env vars

- `MODEL_SERVER_URL`, `MODEL_SERVER_PORT` — API wrapper address (default 18288)
- `S3_BUCKET_NAME` or `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL`, `S3_REGION` — for input_images download
- `BENCHMARK_RUNS` — number of benchmark iterations (default 4). Set to 1 when testing standalone to reduce startup time.
- `BENCHMARK_IMAGE_BUCKET`, `BENCHMARK_IMAGE_KEY` — for I2V benchmark: upload image to S3, e.g. `benchmarks/benchmark_source.jpg`. Run `upload_benchmark_image.sh` in comfy-vast-serverless.
