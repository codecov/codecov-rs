VENV_PATH := $(shell pwd)/.venv/bin:${PATH}

ci.setup_venv:
	python3 -m venv .venv
	echo "${VENV_PATH}" >> ${GITHUB_PATH}
	PATH=${VENV_PATH} pip install -r python/requirements.dev.txt


lint.rust:
	cargo fmt --all --check
	cargo clippy

lint.python:
	ruff check
	ruff format
	mypy
