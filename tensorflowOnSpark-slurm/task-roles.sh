#!/bin/bash
# task-roles.sh

if [[ "$#" -ne 1 ]]; then
  echo "USAGE: BASH $0 LEVEL" 
  exit 1
else
  LEVEL="$1"
fi
echo $(hostname)": $LEVEL : $SLURM_PROCID" 

case $LEVEL in 
  ("client") 
    # Convert the MNIST zip files
    rm -rf examples/mnist/csv
    ${SPARK_HOME}/bin/spark-submit \
      --master ${MASTER_URL} \
      --deploy-mode client \
      --total-executor-cores ${TOTAL_CORES} \
      ${TFoS_HOME}/examples/mnist/mnist_data_setup.py \
      --output examples/mnist/csv \
      --format csv

    ls -lR examples/mnist/csv

    # Run distributed MNIST training (using feed_dict)
    rm -rf mnist_model
    ${SPARK_HOME}/bin/spark-submit \
      --master ${MASTER_URL} \
      --py-files ${TFoS_HOME}/tfspark.zip,${TFoS_HOME}/examples/mnist/spark/mnist_dist.py \
      --conf spark.cores.max=${TOTAL_CORES} \
      --conf spark.task.cpus=${SPARK_WORKER_CORES} \
      --conf spark.executorEnv.JAVA_HOME="$JAVA_HOME" \
      ${TFoS_HOME}/examples/mnist/spark/mnist_spark.py \
      --cluster_size ${SPARK_EXECUTOR_INSTANCES} \
      --images examples/mnist/csv/train/images \
      --labels examples/mnist/csv/train/labels \
      --format csv \
      --mode train \
      --model mnist_model

    ls -l mnist_model


    # Run distributed MNIST inference (using feed_dict)
    rm -rf predictions
    ${SPARK_HOME}/bin/spark-submit \
       --master ${MASTER_URL} \
       --py-files ${TFoS_HOME}/tfspark.zip,${TFoS_HOME}/examples/mnist/spark/mnist_dist.py \
       --conf spark.cores.max=${TOTAL_CORES} \
       --conf spark.task.cpus=${SPARK_WORKER_CORES} \
       --conf spark.executorEnv.JAVA_HOME="$JAVA_HOME" \
       ${TFoS_HOME}/examples/mnist/spark/mnist_spark.py \
       --cluster_size ${SPARK_EXECUTOR_INSTANCES} \
       --images examples/mnist/csv/test/images \
       --labels examples/mnist/csv/test/labels \
       --mode inference \
       --format csv \
       --model mnist_model \
       --output predictions
    
    cat predictions/part-00000
   
    ;;

  ("master") 
    # Start the Spark master
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
      --ip $SPARK_MASTER_HOST
    ;;

  ("worker") 

    SPARK_WORKER_DIR=/tmp/$USER/$JOB_ID/work
    SPARK_LOCAL_DIR=/tmp/$USER/$JOB_ID
    mkdir -p $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    # Start the Worker
    $SPARK_HOME/bin/spark-class \
      org.apache.spark.deploy.worker.Worker $MASTER_URL

    # $SPARK_HOME/sbin/start-slave.sh \
      #   --work-dir $SPARK_WORKER_DIR \
      #   --cores $SPARK_WORKER_CORES \
      #   $MASTER_URL
    ;;

  ("worker-cleanup")
    find /tmp -user $(whoami) -exec rm -r {} \;
    ;;

  (*)
    echo "Invalid LEVEL option [$LEVEL]" 
    exit 1
    ;;
esac

echo $(date) -- $LEVEL
