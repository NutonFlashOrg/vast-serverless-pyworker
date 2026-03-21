.PHONY: lint format check

lint:
	@echo "Linting vast-serverless-pyworker..."
	@ruff check workers/comfyui-json/worker.py workers/comfyui-json/workflow_transform.py scripts/calibrate_workload_timing.py scripts/calibrate_vast_workload_multi_lane.py

format:
	@ruff format workers/comfyui-json/worker.py workers/comfyui-json/workflow_transform.py

check: lint
	@echo "Check passed"
