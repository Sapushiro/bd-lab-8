FROM apache/spark:3.5.1

USER root

WORKDIR /opt/spark/work-dir

COPY requirements-docker.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY config.json .
COPY src/ ./src/

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R 185:0 /home/spark/.ivy2

USER 185