#spark-slurm

Run Spark jobs on ACCRE's traditional HPC cluster via SLURM.

## Description

This directory contains an example Spark job launched through SLURM. The 
job is launched through the SLURM batch script `batch-job.slurm`; this 
script allocates resources to set up tasks consisting of 
1. a client
1. a master 
1. an arbitrary number of workers
Due to how SLURM is structured, the allocation of each of these processes must be
heterogeneous, which is slightly wasteful but easy to understand.

Each process is launched via `srun` where the exectuable for each process
is specified in the `cluster.conf` file.

