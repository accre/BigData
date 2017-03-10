#!/bin/bash
# batch-job.slurm

#SBATCH --ntasks=8
#SBATCH --cpus-per-task=3
#SBATCH --mem-per-cpu=4G
#SBATCH --time=00:30:00
#SBATCH --partition=maxwell
#SBATCH --account=accre_gpu

source job-env.sh

srun --mpi=pmi2 python mpi_jobs.py
