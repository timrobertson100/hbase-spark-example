## HBase IDs

Quick hacks to explore the GBIF HBase table that manages the ID assignment.

Adjust this as necessary:

```
kubectl config use-context production 

export FILE=target/hbase-spark-id-1.0.0-3.5.1.jar
cat $FILE | kubectl exec -i -n production spark-shell-gateway-7f6676b4d5-blcrg "--" sh -c "cat > /tmp/hbase-spark-id-1.0.0-3.5.1.jar"

cd /stackable/spark
./bin/spark-submit \
    --master k8s://https://prodgateway-vh.gbif.org \
    --deploy-mode client \
    --executor-memory 12G \
    --driver-memory 4G \
    --num-executors 21 \
    --executor-cores 5 \
    --packages org.apache.spark:spark-avro_2.12:3.5.1 \
    --conf spark.executor.memoryOverhead=4G \
    --conf spark.kubernetes.authenticate.serviceAccountName="spark-shell-gateway" \
    --conf spark.kubernetes.namespace="production" \
    --conf spark.kubernetes.container.image="stackable-docker.gbif.org/stackable/spark-k8s:3.5.1-stackable24.3.0"\
    --conf spark.driver.host=spark-shell-gateway \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.kubernetes.driver.label.queue=root.uat2.default \
    --conf spark.kubernetes.executor.label.queue=root.uat2.default \
    --conf spark.kubernetes.submit.label.queue=root.uat2.default \
    --conf spark.kubernetes.scheduler.name=yunikorn \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/data/spark/ \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=5Gi \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=local-path-delete \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand \
    --conf spark.driver.port=7078 \
    --conf spark.blockManager.port=7089 \
    --class org.gbif.spark.id.ReadHBaseTable  \
    /tmp/hbase-spark-id-1.0.0-3.5.1.jar
```