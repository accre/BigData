#!/bin/bash

#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:02:00
#SBATCH --mem=100M
#SBATCH --partition=debug

srun -l --multi-prog silly.conf
