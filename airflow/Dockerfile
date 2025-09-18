FROM apache/airflow:2.9.3-python3.10

USER airflow
COPY requirements.txt /requirements.txt
# Dùng constraints đúng phiên bản để tránh bể dependency:
RUN pip install --no-cache-dir -r /requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
