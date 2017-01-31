#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "USAGE: SBATCH [OPTION] $0 LEVEL"; exit 1
fi

LEVEL=$1

# test if level is non-negative number
re='^[0-9]$'
if ! [[ $LEVEL =~ $re ]] ; then
    echo "error: LEVEL must be an integer between 0 and 9,\
        inclusive." >&2; exit 1
fi


if (( LEVEL == 0 )); then

    echo $(hostname)": LEVEL 0"

elif (( LEVEL == 1 )); then
    echo $(hostname)": WORKER" 
    echo $SLURM_NODELIST
    prog="$SPARK_HOME/sbin/start-slave.sh \
        $MASTER_URL"
    echo $prog
    $prog

    scontrol wait_job $CLIENT_JOB_ID

elif (( LEVEL == 2 )); then

    echo $(hostname)": MASTER" 
    export SPARK_MASTER_HOST=$(hostname)
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_WEBUI_PORT=8080
    export MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
    export MASTER_WEBUI_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"
    
    # Write the master url to shared disk so that the client can read it.
    echo $MASTER_URL > $JOB_HOME/master_url

    export SPARK_WORKER_CORES=4
   
    # Start the Spark master
    $SPARK_HOME/sbin/start-master.sh 

    sbatch \
        --nodes=4 \
        --ntasks-per-node=1 \
        --cpus-per-task=$SPARK_WORKER_CORES \
        --mem=10G \
        $SCRIPT_PATH 1 
    
    scontrol wait_job $CLIENT_JOB_ID


elif (( LEVEL == 3 )); then
   
    echo $(hostname)": MASTER" 
    export CLIENT_JOB_ID=$SLURM_JOB_ID
    
    export JOB_HOME="/scratch/$USER/$CLIENT_JOB_ID"
    export SPARK_WORKER_DIR=$JOB_HOME/work
    export SPARK_LOCAL_DIR=$JOB_HOME/tmp

    export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
    mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    export SPARK_WORKER_CORES=1
    export SPARK_DAEMON_MEMORY=5g
    export SPARK_MEMORY=5g

    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export SCRIPT_PATH=$JOB_HOME/$(basename "$0")
    cp "$0" "$SCRIPT_PATH"
    
    sbatch \
        --nodes=1 \
        --mem=10G \
        $SCRIPT_PATH 2

    # Read the master url here 
    MASTER_URL=''
    i=0
    while [ -z "$MASTER_URL" ]; do
        if [[ $i > 100 ]]; then
            exit 1
        fi
        sleep 1s
        if [ -f $JOB_HOME/master_url ]; then
            MASTER_URL=$(head -1 $JOB_HOME/master_url)
        fi
        ((i++))
    done
    echo "Master URL = $MASTER_URL"
   
    # Specify input files
    INPUT="README.md"
    OUTPUT="wordcount_$(date +%Y%m%d_%H%M%S)"
    APP="spark-wc_2.11-1.0.jar $INPUT $OUTPUT"

    # Submit the Spark jar
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER_URL \
        --deploy-mode client \
        $APP 

else

    echo "Should never reach here!"; exit 1

fi
