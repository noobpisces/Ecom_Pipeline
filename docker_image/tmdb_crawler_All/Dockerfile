FROM python:3.9-slim

# Cài đặt Chromium và driver
RUN apt-get update && apt-get install -y \
    wget curl unzip xvfb chromium chromium-driver python3-pip

# Cài thư viện Python cần thiết
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy script crawler
COPY OfficialCraw.py /app/OfficialCraw.py
WORKDIR /app

CMD ["python3", "OfficialCraw.py"]
