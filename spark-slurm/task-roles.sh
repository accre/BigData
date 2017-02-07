#!/bin/bash
# task-roles.sh

if [[ "$#" -ne 1 ]]; then
    echo "USAGE: BASH $0 [LEVEL]"; exit 1
else
    LEVEL="$1"
fi
echo $(hostname)": $LEVEL : $SLURM_PROCID" 


# Reads the master URL from a shared location, indicating that the
# Master process has started
function getMasterURL {
    # Read the master url from shared location 
    MASTER_HOST=''
    i=0
    while [ -z "$MASTER_HOST" ]; do
        if (( $i > 100 )); then
            echo "Starting master timed out"; 
            exit 1
        fi
        sleep 1s

        local flag_path=$JOB_HOME/master_host
        if [ -f $flag_path ]; then
            MASTER_HOST=$(head -1 $flag_path)
        else
            echo "Master host not yet intialized"
        fi
        ((i++))
    done
    
    MASTER_URL="spark://$MASTER_HOST:$SPARK_MASTER_PORT"
}

# Reads files named after worker process IDs
# Once each worker has written its hostname to file, 
# this function terminates
function getPID {
    local i=0

    while (( i < $NWORKERS )); do
        i=0
        for w_procid in `seq 2 $LAST_PROC`; do
            echo "Checking for workers"
            if [ -f $JOB_HOME/$w_procid ]; then
                (( i++ ))
            fi
        done
        sleep 1s
    done

}

if [ $LEVEL == "CLIENT" ]; then

    # Wait for the master to signal back
    getMasterURL
    echo "Master Host: $MASTER_HOST"
    msg1="To tunnel to MasterUI and JobUI -> ssh "
    msg1+="-L $SPARK_MASTER_WEBUI_PORT:$MASTER_HOST:$SPARK_MASTER_WEBUI_PORT "
    msg1+="-L 4040:$MASTER_HOST:4040 "
    msg1+="$USER@login.accre.vanderbilt.edu"
    echo $msg1
    
    # Wait for workers to signal back
    getPID

    # Submit the Spark application, as defined in APP
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER_URL \
        --deploy-mode client \
        --total-executor-cores $(( NWORKERS * $SPARK_EXECUTOR_CORES )) \
        $APP
    
        #--executor-cores $SPARK_EXECUTOR_CORES \
        #--executor-memory $SPARK_EXECUTOR_MEMORY \

elif [ $LEVEL == MASTER ]; then

    export SPARK_MASTER_HOST=$(hostname)
    export MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
    export MASTER_WEBUI_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"

    # Write the master url to shared disk so that the client and workers can read it.
    echo $SPARK_MASTER_HOST > $JOB_HOME/master_host
    
    # Start the Spark master
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT


elif [ $LEVEL == "WORKER" ]; then
    
    getMasterURL
    export SPARK_WORKER_DIR=/tmp/$USER/$JOB_ID/work
    export SPARK_LOCAL_DIR=/tmp/$USER/$JOB_ID
    mkdir -p $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 
    
    echo "Master URL = $MASTER_URL"
    hostname > $JOB_HOME/$SLURM_PROCID 
    # Start the Worker
    "$SPARK_HOME/bin/spark-class" \
        org.apache.spark.deploy.worker.Worker $MASTER_URL

else

    echo "Invalid LEVEL option $LEVEL"; exit 1

fi

echo $(date) -- $LEVEL
