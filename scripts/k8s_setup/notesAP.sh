 # Steps

1. build the image locally
    - $ cd repo root
    - $ docker build -t pyspark_yaetos:latest -f Dockerfile_k8s . # builds from Dockerfile_k8s for arm64 # -> works
    - $ docker buildx build -f Dockerfile_k8s --platform linux/amd64 --tag pyspark_yaetos:latest --load .  # builds from Dockerfile_k8s for amd64 for shipping to ECR # -> works
    - $ docker run -it \
      -h spark \
      pyspark_yaetos \
      bash  # -> works
    - with params
    - yaetos_jobs_home=/Users/aprevot/Synced/github/code/code_perso/yaetos
    - yaetos_home=/Users/aprevot/Synced/github/code/code_perso/yaetos
    - $ docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 \
      -v $yaetos_jobs_home:/mnt/yaetos_jobs \
      -v $yaetos_home:/mnt/yaetos \
      -v $HOME/.aws:/.aws \
      -h spark \
      -w /mnt/yaetos_jobs/ \
      pyspark_yaetos \
      bash
2a. (local) Create a private repository in Docker Desktop.
    # - $ kubectl apply -f scripts/k8s_setup/k8s_setup_spark_master_deployment.yaml  # not useful to spark if submitted with --master k8s://https://kubernetes.docker.internal:6443. Useful to create a spark cluster, then to be targeted from spark-submit with --master spark://https://some_url:port
    # - $ kubectl apply -f scripts/k8s_setup/k8s_setup_spark_worker_deployment.yaml
    # - $ kubectl apply -f scripts/k8s_setup/k8s_setup_spark_master_service.yaml
    - $ kubectl apply -f scripts/k8s_setup/k8s_setup_spark_service_account.yaml
    - $ kubectl apply -f scripts/k8s_setup/k8s_setup_role.yaml
    - $ kubectl apply -f scripts/k8s_setup/k8s_setup_rolebinding.yaml
    - Check status with 
    - $ kubectl get pods
    - $ kubectl get deployments
    - to kill later:
    # - $ kubectl delete -f scripts/k8s_setup/k8s_setup_spark_master_deployment.yaml
    # - $ kubectl delete -f scripts/k8s_setup/k8s_setup_spark_worker_deployment.yaml
    # - $ kubectl delete -f scripts/k8s_setup/k8s_setup_spark_master_service.yaml
    - $ kubectl delete -f scripts/k8s_setup/k8s_setup_spark_service_account.yaml
    - $ kubectl delete -f scripts/k8s_setup/k8s_setup_role.yaml
    - $ kubectl delete -f scripts/k8s_setup/k8s_setup_rolebinding.yaml
    - confirm deletion with 
    - $ kubectl get deployments
    - $ kubectl get services
2b. (cloud) Create a private repository 
    - Done Manually
    - from cmdline: $ aws ecr create-repository --repository-name yaetos_k8s_stg
3b. register to ECR (login to ECR and push to ECR)*
    - ECR_setup.sh
4. send spark-submit
    - see sparksubmit_notesAP.sh
