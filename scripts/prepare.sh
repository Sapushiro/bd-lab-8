#!/bin/bash
set -e

docker run --rm \
  --add-host desktop-control-plane:host-gateway \
  -v ~/.kube:/home/spark/.kube:ro \
  sapushiro/bd-lab-8-data-mart:latest \
  /opt/spark/bin/spark-submit \
  --master k8s://https://desktop-control-plane:54041 \
  --deploy-mode cluster \
  --name openfoodfacts-data-mart-prepare \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,com.typesafe.play:play-json_2.12:2.10.6 \
  --class Main \
  --conf spark.kubernetes.namespace=bd-lab-8 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=sapushiro/bd-lab-8-data-mart:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-data.options.claimName=spark-data \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-data.mount.path=/data \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-data.options.claimName=spark-data \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-data.mount.path=/data \
  local:///opt/app/data-mart.jar \
  prepare