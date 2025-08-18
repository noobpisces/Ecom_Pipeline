FROM python:3.9-slim

RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install notebook pyspark==3.3.2

RUN useradd -m -s /bin/bash -u 1000 jovyan

WORKDIR /home/jovyan

USER jovyan

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--no-browser"]



# Cài đặt các package Python yêu cầu
RUN pip install --no-cache-dir \
    notebook \
    jupyterlab \
    findspark \
    pyspark \
    confluent-kafka \
    delta-spark==2.2.0\
    webdriver-manager\
    selenium\
    streamlit




