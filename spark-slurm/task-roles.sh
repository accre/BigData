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

echo $(hostname)": $LEVEL : $SLURM_PROCID" 


# Reads the master URL from a shared location, indicating that the
# Master process has stared
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

    getMasterURL
    echo "Master Host: $MASTER_HOST"
    echo "To tunnel to WebUI: ssh -L \
        $SPARK_MASTER_PORT:$MASTER_HOST:$SPARK_MASTER_PORT \
        vunetid@login.accre.vanderbilt.edu"
   
    # Wait for workers to signal back
    getPID

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
    
    echo "Master URL = $MASTER_URL"
    hostname > $JOB_HOME/$SLURM_PROCID 
    # Start the Worker
    "$SPARK_HOME/bin/spark-class" \
        org.apache.spark.deploy.worker.Worker $MASTER_URL

else

    echo "Invalid LEVEL option $LEVEL"; exit 1

fi

echo $(date) -- $LEVEL
