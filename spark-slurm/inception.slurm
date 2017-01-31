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
    #$SPARK_HOME/sbin/start-master.sh \
    #$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
    #    --properties-file $HOME/accre/Spark/spark-slurm/spark-defaults.conf 

elif (( LEVEL == 1 )); then

    echo $(hostname)": LEVEL 1" 
    $SPARK_HOME/sbin/start-slave.sh \
        $MASTER_URL
    #"$SPARK_HOME/bin/spark-class" \

elif (( LEVEL == 2 )); then

    export SPARK_MASTER_HOST=$(hostname)
    export SPARK_MASTER_PORT=7077
    export SPARK_MASTER_WEBUI_PORT=8080
    export MASTER_URL="spark://$MASTER_HOST:$MASTER_PORT"
    export MASTER_WEBUI_URL="spark://$MASTER_HOST:$MASTER_PORT"
    
    # Write the master url to shared disk so that the client can read it.
    echo $MASTER_URL > $JOB_HOME/master_url

    N_WORKERS=$(( SLURM_JOB_NUM_NODES - 1 ))
    export SPARK_WORKER_CORES=$SLURM_CPUS_PER_TASK
    
    echo $SLURM_NODELIST
   
    # Start the Spark master
    $SPARK_HOME/sbin/start-master.sh \
        --properties-file $HOME/accre/Spark/spark-slurm/spark-defaults.conf 
   
    #srun -lN1 -r0 $SCRIPT_PATH 0
    srun -lN$N_WORKERS -n$N_WORKERS -r1 $SCRIPT_PATH 1 

elif (( LEVEL == 3 )); then
    
    export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
    export JOB_HOME="/scratch/$USER/$SLURM_JOB_ID"
    export SPARK_WORKER_DIR=$JOB_HOME/work
    export SPARK_LOCAL_DIR=$JOB_HOME/tmp
    mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

    export SPARK_WORKER_CORES=1
    export SPARK_DAEMON_MEMORY=5g
    export SPARK_MEMORY=5g

    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export SCRIPT_PATH=$JOB_HOME/$(basename "$0")
    cp "$0" "$SCRIPT_PATH"
    salloc \
        --nodes=4 \
        --ntasks-per-node=1 \
        --cpus-per-task=$SPARK_WORKER_CORES \
        --mem=10G \
        $SCRIPT_PATH 2 

    # Read the master url here 
    MASTER_URL=''
    i=0
    while [ -z "$MASTER_URL" || $i -lt 100 ]; do
        sleep 1s
        if [ -f $JOB_HOME/master_url ]; then
            MASTER_URL=$(head -1 $JOB_HOME/master_url)
        fi
        ((i++))
    done

    # Specify input files
    INPUT="README.md"
    OUTPUT="wordcount_$(date +%Y%m%d_%H%M%S)"
    APP="spark-wc_2.11-1.0.jar $INPUT $OUTPUT"

    # Submit the Spark jar
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER_URL \
        $APP 


echo Reading input from $input1
echo Writing output to $output_dir
else

    echo "Should never reach here!"; exit 1

fi
