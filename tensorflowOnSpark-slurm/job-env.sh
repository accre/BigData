# Specify input, output, and jar/java/Python files
INPUT="/scratch/$USER/Spark_README.md"
#INPUT="/scratch/$USER/stack-archives/xml/math.stackexchange.com/"
OUTPUT="wordcount_$SLURM_JOB_ID"

# If submitting Scala
export APP="spark-wc_2.11-1.0.jar $INPUT $OUTPUT"

# Create a directory on /scratch to hold some info for this job
export JOB_ID=$SLURM_JOB_ID
export JOB_HOME="/scratch/$USER/$JOB_ID"
mkdir -p $JOB_HOME 

export NWORKERS=$(( $SLURM_NTASKS ))

# Set Spark environment variables
export SPARK_DAEMON_MEMORY=1g
export SPARK_DRIVER_MEMORY=1g

export SPARK_EXECUTOR_INSTANCES=${SLURM_NTASKS} # the number of worker processes per node
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK} # the number of cores per worker
export TOTAL_CORES=$((${SPARK_WORKER_CORES} * ${SPARK_EXECUTOR_INSTANCES}))

export SPARK_WORKER_MEMORY=$(( SLURM_MEM_PER_CPU * $SLURM_CPUS_PER_TASK - 1000))m

# Always launch the master from the first SLURM task
export SPARK_MASTER_HOST=$(hostname)
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
export MASTER_WEBUI_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"


# Load mpi to coordinate the setup of Spark roles
setpkgs -a openmpi_1.10.2

# Load the TensorflowOnSpark package
# sets Anaconda, Tensorflow, JAVA_HOME, SPARK_HOME, TFoSHOME
setpkgs -a tensorflow_on_spark 

# Link the example data to this directory
if [ ! -L mnist ]; 
then
  ln -s ${TFoS_HOME}/mnist 
fi

export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Try to load stuff that the spark scripts will load
source "$SPARK_HOME/sbin/spark-config.sh"
source "$SPARK_HOME/bin/load-spark-env.sh"
source "$SPARK_HOME/conf/spark-env.sh"
