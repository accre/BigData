#!/bin/bash

if [[ "$#" -ne 1 && "$#" -ne 0 ]]; then
    echo "USAGE: SBATCH [OPTION] $0 [LEVEL]"; exit 1
fi
if [ "$#" -eq 1 ]; then
    LEVEL=$1
else
    LEVEL="CLIENT"
fi

echo $(hostname)": $LEVEL" 


if [ $LEVEL == "CLIENT" ]; then
   
    export CLIENT_JOB_ID=$SLURM_JOB_ID
    
    export JOB_HOME="/scratch/$USER/$CLIENT_JOB_ID"
    export SPARK_WORKER_DIR=$JOB_HOME/work
    export SPARK_LOCAL_DIR=$JOB_HOME/tmp
    export JAVA_HOME=/usr/local/java/1.8.0
    export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
    mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    export SPARK_WORKER_CORES=1
    export SPARK_DAEMON_MEMORY=5g
    export SPARK_MEMORY=5g

    # Copy the script directory to a shared location, since SLURM changes the name
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export SCRIPT_PATH=$JOB_HOME/$(basename "$0")
    cp "$0" "$SCRIPT_PATH"
    
    
    # Try to load stuff that the spark scripts will load
    source "$SPARK_HOME/sbin/spark-config.sh"
    source "$SPARK_HOME/bin/load-spark-env.sh"
   
    # Launch a job to start the Master
    MASTER_JOB_ID=$(sbatch \
        --nodes=1 \
        --mem=10G \
        $SCRIPT_PATH MASTER)
    $MASTER_JOB_ID=${MASTER_JOB_ID##Submitted batch job }

    # Read the master url from shared location 
    MASTER_URL=''
    i=0
    while [ -z "$MASTER_URL" ]; do
        if (( $i > 100 )); then
            echo "Starting master timed out"; exit 1
        fi
        sleep 1s
        if [ -f $JOB_HOME/master_url ]; then
            MASTER_URL=$(head -1 $JOB_HOME/master_url)
        fi
        ((i++))
    done
    echo "Master URL = $MASTER_URL"
   
    export MASTER_URL=$MASTER_URL

    # Launch a job to start the Worker nodes
    sbatch \
        --nodes=1 \
        --ntasks-per-node=1 \
        --cpus-per-task=$SPARK_WORKER_CORES \
        --mem=10G \
        $SCRIPT_PATH WORKER 
   
    # Specify input files
    INPUT="README.md"
    OUTPUT="wordcount_$(date +%Y%m%d_%H%M%S)"
    APP="spark-wc_2.11-1.0.jar $INPUT $OUTPUT"
    
    # Submit the Spark jar
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER_URL \
        --deploy-mode client \
        $APP 
    
    # Tear down the master and workers
    scancel $MASTER_JOB_ID
    

elif [ $LEVEL == PROVISION ]; then
    echo "foo"

elif [ $LEVEL == MASTER ]; then

    export SPARK_MASTER_HOST=$(hostname)
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_WEBUI_PORT=8080
    export MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
    export MASTER_WEBUI_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"
    export SPARK_DAEMON_MEMORY=1G
    export SPARK_MEM=$SPARK_DAEMON_MEMORY

    export SPARK_WORKER_CORES=4

    # Try to load stuff that the spark scripts will load
    source "$SPARK_HOME/sbin/spark-config.sh"
    source "$SPARK_HOME/bin/load-spark-env.sh"
    
    # Start the Spark master
    #$SPARK_HOME/sbin/start-master.sh \
    #    --ip $SPARK_MASTER_HOST \
    #    --port $SPARK_MASTER_PORT \
    #    --webui-port $SPARK_MASTER_WEBUI_PORT
        #--properties-file $HOME/accre/Spark/spark-slurm/spark-defaults.conf
    
    # Write the master url to shared disk so that the client can read it.
    echo $MASTER_URL > $JOB_HOME/master_url
    
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT


elif [ $LEVEL == "WORKER" ]; then
    
    echo $SLURM_NODELIST
    #"$SPARK_HOME/sbin/start-slave.sh \
    #    $MASTER_URL"
    
    "$SPARK_HOME/bin/spark-class" \
        org.apache.spark.deploy.worker.Worker $MASTER_URL

else

    echo "Invalid LEVEL option $LEVEL"; exit 1

fi

echo $(date) -- $LEVEL
