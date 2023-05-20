# 베이스 이미지 선택
FROM python:3.9

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        default-jdk \
        build-essential \
        libkrb5-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        wget \
        curl \
        git \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 생성 및 이동
WORKDIR /app

# 필요한 파일 복사
COPY requirements.txt ./

# 필요한 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt
