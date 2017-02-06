#!/bin/bash

#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:02:00
#SBATCH --mem-per-cpu=3G
#SBATCH --partition=maxwell
#SBATCH --account=accre_gpu
    

export JOB_ID=$SLURM_JOB_ID
    
export JOB_HOME="/scratch/$USER/$JOB_ID"
export SPARK_WORKER_DIR=$JOB_HOME/work
export SPARK_LOCAL_DIR=$JOB_HOME/tmp
export JAVA_HOME=/usr/local/java/1.8.0

# setpkgs -a spark_2.1.0
export SPARK_HOME="$HOME/packages/spark-2.1.0-bin-hadoop2.7"
mkdir -p $JOB_HOME $SPARK_WORKER_DIR $SPARK_LOCAL_DIR 

export SPARK_WORKER_CORES=$SLURM_CPUS_PER_TASK
export SPARK_DAEMON_MEMORY=2g
export SPARK_MEMORY=2g

export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
    
# Try to load stuff that the spark scripts will load
source "$SPARK_HOME/sbin/spark-config.sh"
source "$SPARK_HOME/bin/load-spark-env.sh"

srun -l --multi-prog cluster.conf
