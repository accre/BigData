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
    "$SPARK_HOME/sbin/start-master.sh"

elif (( LEVEL == 1 )); then

    echo $(hostname)": LEVEL 1"
    "$SPARK_HOME/sbin/start-slave.sh" \
        --work-dir=$SPARK_WORKER_DIR \
        "spark://$MASTER_SPARK_URL:7077"

elif (( LEVEL == 2 )); then

    export MASTER_SPARK_URL=$(hostname)
    echo $MASTER_SPARK_URL > $JOB_HOME/master_spark_url
    N_WORKERS=$(( SLURM_JOB_NUM_NODES - 1 ))
    export SPARK_WORKER_CORES=$SLURM_CPUS_PER_TASK
    echo $SLURM_NODELIST
   
    srun -lN1 -r0 $SCRIPT_PATH 0
    srun -lN"$N_WORKERS" -r1 $SCRIPT_PATH 1 

elif (( LEVEL == 3 )); then
    
    export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
    export JOB_HOME="/scratch/$USER/$SLURM_JOB_ID"
    export SPARK_WORKER_DIR=$JOB_HOME/work
    export SPARK_LOCAL_DIR=$JOB_HOME/tmp
    mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export SCRIPT_PATH=$JOB_HOME/$(basename "$0")
    cp "$0" "$SCRIPT_PATH"
    sbatch \
        --nodes=4 \
        --mem=10G \
        $SCRIPT_PATH 2 

else

    echo "Should never reach here!"; exit 1

fi
