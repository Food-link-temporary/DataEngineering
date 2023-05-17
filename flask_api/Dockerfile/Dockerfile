# 베이스 이미지 선택
FROM python:3.9-slim

# 작업 디렉토리 생성 및 이동
WORKDIR /app

# 필요한 파일 복사
COPY requirements.txt ./
COPY app.py ./

# 필요한 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# Flask 애플리케이션 실행
CMD [ "python", "app.py" ]
