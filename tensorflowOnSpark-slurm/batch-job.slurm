#!/bin/bash
# batch-job.slurm

#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=16G
#SBATCH --time=01:00:00
#SBATCH --partition=maxwell
#SBATCH --account=accre_gpu

source job-env.sh

# ./task-roles.sh master
# sleep 3
# 
# srun -n2 ./task-roles.sh worker
# 
# sleep 3
# echo "keep on"
# 
# ./task-roles.sh client

srun --mpi=pmi2 ./mpi_jobs.py
