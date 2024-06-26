# Orig From chatGPT
./bin/spark-submit \
    --master k8s://https://<k8s-api-server>:<port> \
    --deploy-mode cluster \
    --name <job-name> \
    --class <main-class> \
    --conf spark.executor.instances=<number-of-instances> \
    --conf spark.kubernetes.container.image=<spark-image> \
    --conf spark.kubernetes.namespace=default \
    --conf spark.kubernetes.driver.pod.name=<pod-name> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    <application-jar> \
    [application-arguments]

# And for pyspark job
./bin/spark-submit \
    --master k8s://https://kubernetes.docker.internal:6443 \
    --deploy-mode cluster \
    --name my-pyspark-job \
    --conf spark.kubernetes.namespace=default \
    --conf spark.kubernetes.container.image=<custom-spark-image> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.pyspark.pythonVersion=3 \
    --conf spark.kubernetes.driver.pod.name=my-pyspark-pod \
    --py-files local:///path/to/dependencies.zip \
    --conf spark.kubernetes.file.upload.path=s3://my-bucket/spark-jobs/ \
    local:///path/to/your_script.py \
    [application arguments]

# For basic spark job, in local kubernetes (using Docker Desktop)
spark-submit \
    --master k8s://https://kubernetes.docker.internal:6443 \
    --deploy-mode cluster \
    --name my-pyspark-job \
    --conf spark.kubernetes.namespace=default \
    --conf spark.kubernetes.container.image=pyspark_yaetos \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-service-account \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.pyspark.pythonVersion=3 \
    --conf spark.pyspark.python=python3 \
    --conf spark.pyspark.driver.python=python3 \
    --conf spark.kubernetes.driver.pod.name=my-pyspark-pod \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.mount.path=/data \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/scripts/k8s_setup/ \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir.options.path=/Users/aprevot/Synced/github/code/code_perso/yaetos/scripts/k8s_setup/ \
    --conf spark.kubernetes.file.upload.path=file:///data/ \
    local:///data/sample_spark_job.py
    # works.

    # Params removed
    # --conf spark.kubernetes.authenticate.driver.serviceAccountName=default \
    # --conf spark.kubernetes.driver.pod.name=my-pyspark-pod \
    # --conf spark.kubernetes.file.upload.path=s3://my-bucket/spark-jobs/ \
    # local:///mnt/yaetos_jobs/scripts/k8s_setup/sample_spark_job.py

kubectl logs my-pyspark-pod -n default
kubectl get pods
kubectl delete pod my-pyspark-pod -n default
