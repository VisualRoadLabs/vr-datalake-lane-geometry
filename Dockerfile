FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN apt-get update \
  && apt-get upgrade -y \
  && rm -rf /var/lib/apt/lists/* \
  && python -m pip install --no-cache-dir --upgrade \
    pip \
    "setuptools>=80.9.0" \
    "wheel>=0.46.3" \
    "jaraco.context>=6.1.2" \
  && find /usr/lib -path "*/gconv/IBM1390.so" -delete \
  && find /usr/lib -path "*/gconv/IBM1399.so" -delete \
  && python -m pip install --no-cache-dir .

ENTRYPOINT ["lane-geometry-job"]
