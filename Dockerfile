FROM ghcr.nju.edu.cn/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0 UV_INDEX_URL=http://mirrors.aliyun.com/pypi/simple/

WORKDIR /app
ADD . /app
RUN uv sync

ENV PATH="/app/.venv/bin:$PATH"

CMD ["streamlit", "run", "main.py"]