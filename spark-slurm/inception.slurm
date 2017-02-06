#!/bin/bash
# spark.slurm

if [[ "$#" -ne 1 && "$#" -ne 0 ]]; then
    echo "USAGE: SBATCH [OPTION] $0 [LEVEL]"; exit 1
fi
if [ "$#" -eq 1 ]; then
    LEVEL=$1
else
    LEVEL="CLIENT"
fi

echo $(hostname)": $LEVEL" 

# make sure you have enough time allocated in the first invocation
export JOB_TIME="00:30:00"

if [ $LEVEL == "CLIENT" ]; then
   
    export CLIENT_JOB_ID=$SLURM_JOB_ID
    
    export JOB_HOME="/scratch/$USER/$CLIENT_JOB_ID"
    export SPARK_WORKER_DIR=$JOB_HOME/work
    export SPARK_LOCAL_DIR=$JOB_HOME/tmp
    export JAVA_HOME=/usr/local/java/1.8.0
    export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
    mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    export SPARK_WORKER_CORES=4
    export SPARK_DAEMON_MEMORY=2g
    export SPARK_MEMORY=2g
    
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_WEBUI_PORT=8080

    # Copy the script directory to a shared location, since SLURM changes the name
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export SCRIPT_PATH=$JOB_HOME/$(basename "$0")
    cp "$0" "$SCRIPT_PATH"
    
    
    # Try to load stuff that the spark scripts will load
    source "$SPARK_HOME/sbin/spark-config.sh"
    source "$SPARK_HOME/bin/load-spark-env.sh"
   
    # Launch a job to start the Master
    MASTER_JOB_ID=$(sbatch \
        --time=$JOB_TIME \
        --nodes=1 \
        --mem=10G \
        $SCRIPT_PATH MASTER)
    echo $MASTER_JOB_ID
    MASTER_JOB_ID=${MASTER_JOB_ID//[!0-9]/} 
   
    # Read the master url from shared location 
    MASTER_HOST=''
    i=0
    while [ -z "$MASTER_HOST" ]; do
        if (( $i > 100 )); then
            echo "Starting master timed out"; 
            scancel $MASTER_JOB_ID;
            exit 1
        fi
        sleep 1s
        if [ -f $JOB_HOME/master_url ]; then
            MASTER_HOST=$(head -1 $JOB_HOME/master_host)
        fi
        ((i++))
    done
    echo "To tunnel to WebUI: ssh -L \
        $SPARK_MASTER_PORT:$MASTER_HOST:$SPARK_MASTER_PORT \
        vunetid@login.accre.vanderbilt.edu"

    # necessary to start a worker
    export MASTER_URL="spark://$MASTER_HOST:$SPARK_MASTER_PORT"
    echo "Master URL = $MASTER_URL"
     


    # Launch a job to start the Worker nodes
    export MASTER_URL=$MASTER_URL 

    # Since Spark uses ssh in standalone cluster mode, there's
    # no need to invoke MPI
    WORKERS_JOB_ID=$(sbatch \
        --time=$JOB_TIME \
        --nodes=1 \
        --ntasks-per-node=1 \
        --cpus-per-task=$SPARK_WORKER_CORES \
        --mem=10G \
        $SCRIPT_PATH WORKER)
    echo $WORKERS_JOB_ID
    WORKERS_JOB_ID=${WORKERS_JOB_ID//[!0-9]/} 
   
    # TODO: Optionally, wait for workers to signal back
    sleep 5s

    # Specify input files
    # INPUT="README.md"
    INPUT="/scratch/arnoldjr/stack-archives/xml/ai.stackexchange.com/"
    OUTPUT="wordcount_$(date +%Y%m%d_%H%M%S)"
    APP="spark-wc_2.11-1.0.jar $INPUT $OUTPUT"
    
    # Submit the Spark jar
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER_URL \
        --deploy-mode client \
        $APP 
    
    echo $INPUT
    echo $OUTPUT
    echo $APP
    
    # Tear down the master and workers
    scancel $MASTER_JOB_ID
    scancel $WORKERS_JOB_ID
    

elif [ $LEVEL == MASTER ]; then

    export SPARK_MASTER_HOST=$(hostname)
    export MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
    export MASTER_WEBUI_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"

    # Try to load stuff that the spark scripts will load
    source "$SPARK_HOME/sbin/spark-config.sh"
    source "$SPARK_HOME/bin/load-spark-env.sh"
    
    # Write the master url to shared disk so that the client can read it.
    echo $SPARK_MASTER_HOST > $JOB_HOME/master_host
    
    # Start the Spark master
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT


elif [ $LEVEL == "WORKER" ]; then
    
    # Start the Worker
    "$SPARK_HOME/bin/spark-class" \
        org.apache.spark.deploy.worker.Worker $MASTER_URL

else

    echo "Invalid LEVEL option $LEVEL"; exit 1

fi

echo $(date) -- $LEVEL
