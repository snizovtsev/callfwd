FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    libsystemd-dev \
    libssl-dev \
    libevent-dev \
    libsodium-dev \
    gperf \
    python3 \
    && rm -rf /var/lib/apt/lists/*
