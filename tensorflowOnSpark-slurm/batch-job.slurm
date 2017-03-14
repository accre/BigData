#!/bin/bash
# batch-job.slurm

#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=3
#SBATCH --mem-per-cpu=8G
#SBATCH --time=00:10:00
#SBATCH --partition=maxwell
#SBATCH --account=accre_gpu

source job-env.sh

srun --mpi=pmi2 ./mpi_jobs.py
