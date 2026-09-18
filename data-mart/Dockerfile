FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/app

COPY data-mart/target/scala-2.12/openfoodfacts-data-mart_2.12-0.1.0.jar /opt/app/data-mart.jar
COPY model/config.json /opt/app/config.json

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R 185:0 /home/spark/.ivy2 /opt/app

USER 185